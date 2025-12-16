import csv
import io
import json
import logging
import os
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict
import time

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import OperationalError as SAOperationalError
try:
    import psycopg2
    Psycopg2OperationalError = psycopg2.OperationalError
except Exception:
    Psycopg2OperationalError = Exception

# Import from local modules (AlwaysOnPC standalone)
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))

from database_models import get_session, VestrFeeRecord, FeeSyncStatus, ensure_fee_tables, VestrFeeMonthlySummary, VestrFeeDailySummary, VestrFeeProductTotal
from vestr_lightweight import LightweightVestrScraper

logger = logging.getLogger(__name__)

# Simple in-memory cache for fees data (avoid repeated API calls)
_fees_cache: Dict[str, Tuple[datetime, Any, Dict[str, Any]]] = {}
_CACHE_TTL_SECONDS = 300  # 5 minutes cache

FEES_CACHE_DIR = os.path.join(PROJECT_ROOT, "uploads", "cache")
FEES_CACHE_FILE = os.path.join(FEES_CACHE_DIR, "vestr_fees_cache.json")
FEES_CACHE_MAX_AGE = timedelta(hours=24)
DATA_STALE_DAYS = 1
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


MAX_INCREMENTAL_PAGES = max(1, _env_int("FEE_SYNC_MAX_INCREMENTAL_PAGES", 25))
INCREMENTAL_LOOKBACK_DAYS = max(1, _env_int("FEE_SYNC_LOOKBACK_DAYS", 30))
DEFAULT_PAGE_SIZE = max(500, _env_int("FEE_SYNC_PAGE_SIZE", 5000))
# Number of rows to insert per DB batch during upsert. Smaller batches reduce chance
# of a single large transaction being interrupted by transient network/SSL issues.
FEE_SYNC_INSERT_BATCH_SIZE = max(100, _env_int("FEE_SYNC_INSERT_BATCH_SIZE", 1000))
# How many times to retry a failing batch insert (transient DB errors).
FEE_SYNC_INSERT_RETRY_MAX = max(1, _env_int("FEE_SYNC_INSERT_RETRY_MAX", 5))

_DB_SYNC_LOCK = threading.Lock()
_ASYNC_SYNC_IN_PROGRESS = threading.Event()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        cleaned = raw.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        return datetime.fromisoformat(cleaned)
    except Exception:
        return None


def _format_iso_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _load_disk_cache() -> Optional[Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
    if not os.path.exists(FEES_CACHE_FILE):
        return None
    try:
        with open(FEES_CACHE_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        fetched_at = _parse_iso_datetime(payload.get("fetched_at"))
        if not fetched_at:
            return None
        age = _utcnow() - fetched_at
        if age > FEES_CACHE_MAX_AGE:
            logger.info("Cached fees data expired (age=%s)", age)
            return None
        items = payload.get("items", [])
        meta = {
            "fetched_at": fetched_at,
            "source": "disk",
            "record_count": len(items),
        }
        return items, meta
    except Exception as exc:
        logger.warning("Failed to load fees cache from disk: %s", exc)
        return None


def _write_disk_cache(items: List[Dict[str, Any]], fetched_at: datetime) -> None:
    try:
        os.makedirs(FEES_CACHE_DIR, exist_ok=True)
        payload = {
            "fetched_at": _format_iso_datetime(fetched_at),
            "record_count": len(items),
            "items": items,
        }
        with open(FEES_CACHE_FILE, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
    except Exception as exc:
        logger.warning("Failed to write fees cache to disk: %s", exc)


def _month_key_iter(start: date, end: date) -> List[str]:
    """Return list of YYYY-MM keys between start and end (inclusive)."""
    if start > end:
        start, end = end, start
    keys: List[str] = []
    year, month = start.year, start.month
    end_year, end_month = end.year, end.month

    while True:
        keys.append(f"{year:04d}-{month:02d}")
        if year == end_year and month == end_month:
            break
        month += 1
        if month > 12:
            month = 1
            year += 1
    return keys


def _select_recent_fee_rows(
    records: List[Dict[str, Any]],
    *,
    prefer_previous_day: bool = True,
    current_date: Optional[date] = None,
) -> Tuple[List[Dict[str, Any]], Optional[date]]:
    """Return rows that belong to the preferred "recent" day.

    The UI expects to highlight "yesterday" when data exists for that date,
    otherwise it should gracefully fall back to the latest available day.
    """
    if not records:
        return [], None

    ordered = sorted(records, key=lambda r: r["date"], reverse=True)
    today = current_date or datetime.utcnow().date()
    candidate_dates: List[date] = []

    if prefer_previous_day:
        candidate_dates.append(today - timedelta(days=1))
    candidate_dates.append(today)

    seen = set(candidate_dates)
    for row in ordered:
        row_date = row.get("row_date")
        if row_date and row_date not in seen:
            candidate_dates.append(row_date)
            seen.add(row_date)

    target_date: Optional[date] = None
    for candidate in candidate_dates:
        if candidate and any(r.get("row_date") == candidate for r in ordered):
            target_date = candidate
            break

    if not target_date:
        target_date = ordered[0].get("row_date")

    if not target_date:
        return [], None

    filtered = [r for r in ordered if r.get("row_date") == target_date]
    return filtered, target_date



class LightweightVestrFeesScraper(LightweightVestrScraper):
    """Scrape the Vestr Fees page using GraphQL API and persist results locally.

    Design principles after persistence upgrade:
    - Fetch everything once, then keep a rolling incremental window synced to PostgreSQL
    - Prefer cached/database data for dashboards and CSV exports (sub-second responses)
    - Fall back to live GraphQL pulls only when cache is empty or explicitly forced
    - Keep a small disk cache for CLI/debug usage if the database is unavailable
    """

    FEES_PAGE_URL = "https://aisfg.delta.vestr.com/fees"
    GRAPHQL_URL = "https://aisfg.delta.vestr.com/graphql"
    DEFAULT_MIN_GROUP_DATE = datetime(2023, 2, 18)

    # GraphQL query for fee deductions
    FEE_DEDUCTIONS_QUERY = """
    query FeeDeductionsQuery($limit: Int!, $offset: Int) {
      feeDeductions(limit: $limit, offset: $offset) {
        items {
          id
          product {
            id
            name
            isin
          }
          currency
          type
          beneficiaryId
          outstandingQuantity
          positionChange
          bookingDate
          feeName
        }
        totalCount
      }
    }
    """

    # Fee types
    FEE_TYPE_MANAGEMENT = "ManagementFeeDeduction"
    FEE_TYPE_PERFORMANCE = "PerformanceFeeDeduction"
    FEE_TYPE_CUSTODY = "CustodyFeeDeduction"
    ALL_FEE_TYPES = [FEE_TYPE_MANAGEMENT, FEE_TYPE_PERFORMANCE, FEE_TYPE_CUSTODY]

    def __init__(self, max_pages: int = 1000):
        super().__init__()
        self.max_pages = max(1, max_pages)
        self.csrf_token = None
        self._fees_logged_in = False

    def login(self):
        result = super().login()
        self._fees_logged_in = True
        return result

    def _ensure_csrf_token(self) -> None:
        """Get CSRF token from session cookies."""
        if self.csrf_token:
            return
        
        self.csrf_token = self.session.cookies.get("csrf-token")
        
        if not self.csrf_token:
            logger.info("No CSRF token in cookies, accessing fees page...")
            resp = self.session.get(self.FEES_PAGE_URL, timeout=30, allow_redirects=True)
            if "auth" in resp.url or "login" in resp.url:
                raise Exception("Not authenticated for fees page")
            resp.raise_for_status()
            
            self.csrf_token = self.session.cookies.get("csrf-token")
            
            if not self.csrf_token:
                self.csrf_token = self.session.cookies.get("XSRF-TOKEN") or self.session.cookies.get("csrf_token")
            
            if not self.csrf_token:
                import re
                match = re.search(r'"csrfToken"\s*:\s*"([^"]+)"', resp.text)
                if match:
                    self.csrf_token = match.group(1)
        
        if not self.csrf_token:
            raise Exception("Could not obtain CSRF token")
        
        logger.info("CSRF token obtained: Yes")

    # ------------------------------------------------------------------
    # Database persistence helpers
    # ------------------------------------------------------------------

    def _get_database_session(self):
        try:
            ensure_fee_tables()
            return get_session()
        except Exception as exc:
            logger.warning("Cannot establish DB session: %s", exc)
            raise

    def _get_or_create_sync_status(self, session) -> FeeSyncStatus:
        status = session.query(FeeSyncStatus).order_by(FeeSyncStatus.id.asc()).first()
        if status:
            return status
        status = FeeSyncStatus(status='idle', last_record_count=0)
        session.add(status)
        session.commit()
        return status

    def _get_database_stats(self, session) -> Dict[str, Any]:
        try:
            record_count = session.query(func.count(VestrFeeRecord.id)).scalar() or 0
            latest_booking = session.query(func.max(VestrFeeRecord.booking_date)).scalar()
            last_updated = session.query(func.max(VestrFeeRecord.updated_at)).scalar()
            status_row = self._get_or_create_sync_status(session)
            today = datetime.utcnow().date()
            is_stale = False
            if latest_booking:
                is_stale = latest_booking <= (today - timedelta(days=DATA_STALE_DAYS))
            stats = {
                "record_count": record_count,
                "latest_booking_date": latest_booking,
                "last_sync_time": status_row.last_incremental_sync or last_updated,
                "last_run_mode": status_row.last_run_mode,
                "last_sync_status": status_row.status,
                "last_sync_error": status_row.last_error,
                "has_data": record_count > 0,
                "is_stale": is_stale,
                "status_row_id": status_row.id,
            }
            return stats
        except Exception as exc:
            logger.warning("DB stats query failed: %s", exc)
            raise

    def _trigger_async_sync(self):
        if _ASYNC_SYNC_IN_PROGRESS.is_set():
            return

        def _job():
            try:
                _ASYNC_SYNC_IN_PROGRESS.set()
                scraper = LightweightVestrFeesScraper(max_pages=self.max_pages)
                scraper.login()
                scraper._sync_database_with_remote(full_refresh=False)
            except Exception as exc:
                logger.warning("Background fee sync failed: %s", exc, exc_info=True)
            finally:
                _ASYNC_SYNC_IN_PROGRESS.clear()

        threading.Thread(target=_job, daemon=True).start()

    def _load_fees_from_database(
        self,
        min_date: date,
        max_date: date,
        fee_types: Optional[List[str]],
        force_refresh: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        try:
            session = self._get_database_session()
        except Exception as exc:
            logger.warning("DB session failed, cannot load from database: %s", exc)
            raise

        try:
            stats = self._get_database_stats(session)
        except Exception as exc:
            session.close()
            logger.warning("DB stats query failed: %s", exc)
            raise

        # Only sync if explicitly requested via force_refresh or if DB is empty
        # NEVER force-sync on dashboard loads when DB has data
        if force_refresh and not stats["has_data"]:
            session.close()
            logger.info("DB is empty and force_refresh=True, triggering initial full sync")
            try:
                self.login()
                self._sync_database_with_remote(full_refresh=True)
                session = self._get_database_session()
                stats = self._get_database_stats(session)
            except Exception as sync_exc:
                logger.error("Initial sync failed: %s", sync_exc)
                raise
        elif stats["is_stale"]:
            # Trigger background sync but don't wait
            self._trigger_async_sync()

        try:
            query = session.query(VestrFeeRecord).filter(
                VestrFeeRecord.booking_date >= min_date,
                VestrFeeRecord.booking_date <= max_date,
            )
            if fee_types:
                query = query.filter(VestrFeeRecord.fee_type.in_(fee_types))

            rows = query.order_by(VestrFeeRecord.booking_datetime.desc()).all()
            items = [row.to_item() for row in rows]
        except Exception as exc:
            session.close()
            logger.error("DB query failed: %s", exc)
            raise
        finally:
            session.close()

        meta = {
            "record_count": stats["record_count"],
            "fetched_at": stats["last_sync_time"],
            "source": "database",
            "run_mode": stats.get("last_run_mode"),
            "status": stats.get("last_sync_status"),
            "status_error": stats.get("last_sync_error"),
        }
        return items, meta

    def _sync_database_with_remote(self, full_refresh: bool = False) -> Dict[str, Any]:
        if not self._fees_logged_in:
            self.login()

        if not _DB_SYNC_LOCK.acquire(blocking=False):
            logger.info("Fee sync already running, skipping duplicate request")
            return {"running": True}

        start_time = datetime.utcnow()
        total_processed = 0
        last_fee_id: Optional[str] = None
        latest_booking_seen: Optional[date] = None
        data_session = self._get_database_session()
        status_session = self._get_database_session()
        stats_session = self._get_database_session()

        try:
            try:
                stats_before = self._get_database_stats(stats_session)
            finally:
                stats_session.close()

            sync_mode = "full" if full_refresh or not stats_before["has_data"] else "incremental"
            page_limit = self.max_pages if sync_mode == "full" else min(self.max_pages, MAX_INCREMENTAL_PAGES)
            min_sync_date: Optional[date] = None
            if sync_mode == "incremental" and stats_before["latest_booking_date"]:
                min_sync_date = stats_before["latest_booking_date"] - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)

            status_row = self._get_or_create_sync_status(status_session)
            status_row.last_sync_started_at = start_time
            status_row.status = "running"
            status_row.last_run_mode = sync_mode
            status_session.commit()

            pages_used = 0
            for page_items in self._iter_remote_fee_pages(
                page_size=DEFAULT_PAGE_SIZE,
                max_pages=page_limit,
                stop_before_date=min_sync_date,
            ):
                pages_used += 1
                logger.info("Processing page %d: preparing %d items...", pages_used, len(page_items))
                rows = self._prepare_fee_rows(page_items, min_booking_date=min_sync_date)
                if not rows:
                    logger.info("Page %d: no rows to upsert (filtered out)", pages_used)
                    continue
                logger.info("Page %d: upserting %d rows to database...", pages_used, len(rows))
                self._bulk_upsert_rows(data_session, rows)
                total_processed += len(rows)
                logger.info("Page %d complete: %d rows processed (total so far: %d)", pages_used, len(rows), total_processed)
                last_fee_id = rows[0].get("fee_id") or last_fee_id
                booking_date = rows[0].get("booking_date")
                if booking_date:
                    if isinstance(booking_date, datetime):
                        booking_day = booking_date.date()
                    else:
                        booking_day = booking_date
                    if not latest_booking_seen or booking_day > latest_booking_seen:
                        latest_booking_seen = booking_day

            data_session.commit()

            # Update summaries ONCE for only the latest dates (not all historical dates)
            logger.info("Updating daily/monthly summaries for latest dates...")
            try:
                # Get the latest 3 booking dates that were just synced
                latest_dates = data_session.query(VestrFeeRecord.booking_date).distinct().order_by(VestrFeeRecord.booking_date.desc()).limit(3).all()
                latest_date_set = {d[0] for d in latest_dates if d[0]}
                if latest_date_set:
                    logger.info("Updating summaries for %d latest dates: %s", len(latest_date_set), sorted(latest_date_set, reverse=True))
                    self._upsert_monthly_summaries(data_session, latest_date_set)
                    data_session.commit()
                    logger.info("Summary update complete")
            except Exception as sum_exc:
                logger.error("Failed to update summaries: %s", sum_exc, exc_info=True)
                data_session.rollback()

            record_count = data_session.query(func.count(VestrFeeRecord.id)).scalar() or 0
            latest_booking_date = data_session.query(func.max(VestrFeeRecord.booking_date)).scalar() or latest_booking_seen

            status_row = self._get_or_create_sync_status(status_session)
            status_row.mark_sync(
                mode=sync_mode,
                record_count=record_count,
                latest_booking=latest_booking_date,
                last_fee_id=last_fee_id,
                duration_seconds=(datetime.utcnow() - start_time).total_seconds(),
            )
            status_session.commit()

            logger.info(
                "✅ Fee database %s sync complete: %d rows processed (total=%d, pages=%d)",
                sync_mode,
                total_processed,
                record_count,
                pages_used,
            )
            return {
                "processed": total_processed,
                "record_count": record_count,
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "mode": sync_mode,
                "pages": pages_used,
            }
        except Exception as exc:
            data_session.rollback()
            status_session.rollback()
            try:
                status_row = self._get_or_create_sync_status(status_session)
                status_row.mark_failure(str(exc))
                status_session.commit()
            except Exception:
                status_session.rollback()
            logger.error("[ERROR] Fee database sync failed: %s", exc, exc_info=True)
            raise
        finally:
            data_session.close()
            status_session.close()
            _DB_SYNC_LOCK.release()

    def _iter_remote_fee_pages(
        self,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = None,
        stop_before_date: Optional[date] = None,
    ):
        offset = 0
        total_count = None
        max_allowed_pages = max_pages or self.max_pages
        logger.info("Starting to fetch fees from Vestr API (page_size=%d, max_pages=%d)", page_size, max_allowed_pages)
        for page in range(max_allowed_pages):
            logger.info("Fetching page %d (offset=%d)...", page + 1, offset)
            variables = {"limit": page_size, "offset": offset}
            data = self._post_graphql_fees(self.FEE_DEDUCTIONS_QUERY, variables)
            fees_node = data.get("feeDeductions", {})
            batch = fees_node.get("items", [])
            total_count = total_count or fees_node.get("totalCount")
            logger.info("Page %d fetched: %d items (total_count=%s)", page + 1, len(batch), total_count)
            if not batch:
                logger.info("No more items, stopping pagination")
                break
            yield batch
            offset += page_size
            if total_count and offset >= total_count:
                break
            if len(batch) < page_size:
                break
            if stop_before_date:
                oldest_batch_date: Optional[date] = None
                for entry in batch:
                    booking_dt = self._parse_date_value(entry.get("bookingDate"))
                    if not booking_dt:
                        continue
                    booking_day = booking_dt.date()
                    if oldest_batch_date is None or booking_day < oldest_batch_date:
                        oldest_batch_date = booking_day
                if oldest_batch_date and oldest_batch_date < stop_before_date:
                    break

    def _prepare_fee_rows(
        self,
        items: List[Dict[str, Any]],
        min_booking_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        now = datetime.utcnow()
        for item in items:
            booking_dt = self._parse_date_value(item.get("bookingDate"))
            if not booking_dt:
                continue
            if min_booking_date and booking_dt.date() < min_booking_date:
                continue
            product = item.get("product", {}) or {}
            try:
                position_change = float(item.get("positionChange") or 0)
            except Exception:
                position_change = 0.0
            amount_abs = abs(position_change)
            fee_name = item.get("feeName") or (item.get("type", "").replace("FeeDeduction", " Fee"))
            beneficiary_id = item.get("beneficiaryId")
            if beneficiary_id is not None:
                beneficiary_id = str(beneficiary_id)
            outstanding_quantity = item.get("outstandingQuantity")
            try:
                if outstanding_quantity is not None:
                    outstanding_quantity = float(str(outstanding_quantity).replace(",", ""))
            except Exception:
                outstanding_quantity = None

            rows.append(
                {
                    "fee_id": str(item.get("id")),
                    "product_uid": str(product.get("id")) if product.get("id") else None,
                    "product_name": product.get("name"),
                    "product_isin": product.get("isin"),
                    "currency": item.get("currency"),
                    "fee_type": item.get("type"),
                    "fee_name": fee_name,
                    "beneficiary_id": beneficiary_id,
                    "outstanding_quantity": outstanding_quantity,
                    "position_change": position_change,
                    "amount_abs": amount_abs,
                    "booking_datetime": booking_dt,
                    "booking_date": booking_dt.date(),
                    "raw_payload": json.dumps(item, ensure_ascii=False),
                    "synced_at": now,
                    "updated_at": now,
                }
            )
        return rows

    def _bulk_upsert_rows(self, session, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        dialect = session.bind.dialect.name

        def _chunked(iterable: List[Dict[str, Any]], size: int):
            for i in range(0, len(iterable), size):
                yield iterable[i : i + size]

        if dialect == "postgresql":
            # Split into smaller batches to avoid very large single INSERT statements
            chunks = list(_chunked(rows, FEE_SYNC_INSERT_BATCH_SIZE))
            logger.info("Upserting %d rows in %d chunks (chunk_size=%d)...", len(rows), len(chunks), FEE_SYNC_INSERT_BATCH_SIZE)
            for chunk_idx, batch in enumerate(chunks, 1):
                attempts = 0
                while True:
                    try:
                        logger.info("  Chunk %d/%d: inserting %d rows...", chunk_idx, len(chunks), len(batch))
                        stmt = insert(VestrFeeRecord).values(batch)
                        update_cols = {col: stmt.excluded[col] for col in [
                            "product_uid",
                            "product_name",
                            "product_isin",
                            "currency",
                            "fee_type",
                            "fee_name",
                            "beneficiary_id",
                            "outstanding_quantity",
                            "position_change",
                            "amount_abs",
                            "booking_datetime",
                            "booking_date",
                            "raw_payload",
                            "synced_at",
                            "updated_at",
                        ]}
                        stmt = stmt.on_conflict_do_update(index_elements=[VestrFeeRecord.fee_id], set_=update_cols)
                        session.execute(stmt)
                        logger.info("  Chunk %d/%d: rows inserted", chunk_idx, len(chunks))
                        # commit per-batch to keep transactions small and durable
                        try:
                            logger.info("  Chunk %d/%d: committing...", chunk_idx, len(chunks))
                            session.commit()
                            logger.info("  Chunk %d/%d: DONE", chunk_idx, len(chunks))
                        except Exception:
                            session.rollback()
                            raise
                        break
                    except (SAOperationalError, Psycopg2OperationalError) as db_exc:
                        attempts += 1
                        if attempts > FEE_SYNC_INSERT_RETRY_MAX:
                            logger.error("DB batch insert failed after %d attempts: %s", attempts, db_exc)
                            raise
                        backoff = 2 ** (attempts - 1)
                        logger.warning(
                            "Transient DB error on batch insert (attempt %d/%d): %s — retrying in %ds",
                            attempts,
                            FEE_SYNC_INSERT_RETRY_MAX,
                            db_exc,
                            backoff,
                        )
                        try:
                            session.rollback()
                        except Exception:
                            pass
                        time.sleep(backoff)
        else:
            # Fallback for SQLite/dev environments — merge one-by-one
            for row in rows:
                session.merge(VestrFeeRecord(**row))
            try:
                session.commit()
            except Exception:
                session.rollback()
                raise

    def _post_graphql_fees(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute GraphQL query with fresh session to avoid connection issues."""
        self._ensure_csrf_token()
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-csrf-token": self.csrf_token,
            "Connection": "close",
        }
        
        payload = {
            "query": query,
            "operationName": "FeeDeductionsQuery",
        }
        if variables:
            payload["variables"] = variables
        
        import requests
        fresh_session = requests.Session()
        fresh_session.cookies.update(self.session.cookies)
        
        try:
            resp = fresh_session.post(self.GRAPHQL_URL, json=payload, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        finally:
            fresh_session.close()
        
        if "errors" in data and data["errors"]:
            raise Exception(f"GraphQL errors: {data['errors']}")
        
        return data.get("data", {})

    def _fetch_all_fees(self, force_refresh: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Fetch all fee records, preferring cache unless forced."""
        global _fees_cache

        cache_key = "raw_fees"
        now = datetime.now()

        if not force_refresh:
            mem_entry = _fees_cache.get(cache_key)
            if mem_entry:
                cached_time, cached_data, cached_meta = mem_entry
                if (now - cached_time).total_seconds() < _CACHE_TTL_SECONDS:
                    logger.info("Using in-memory cached fees data (%d items)", len(cached_data))
                    return cached_data, cached_meta

            disk_entry = _load_disk_cache()
            if disk_entry:
                items, meta = disk_entry
                _fees_cache[cache_key] = (now, items, meta)
                logger.info("Using disk cached fees data (%d items)", len(items))
                return items, meta

        items = self._fetch_all_fees_remote()
        fetched_at = _utcnow()
        meta = {
            "fetched_at": fetched_at,
            "source": "remote",
            "record_count": len(items),
        }
        _fees_cache[cache_key] = (now, items, meta)
        _write_disk_cache(items, fetched_at)
        return items, meta

    def _fetch_all_fees_remote(self, page_size: int = DEFAULT_PAGE_SIZE) -> List[Dict[str, Any]]:
        """Fetch fees directly from Vestr GraphQL API using pagination."""
        if not self._fees_logged_in:
            self.login()
        items: List[Dict[str, Any]] = []
        offset = 0
        total_count = None

        for page in range(self.max_pages):
            variables = {"limit": page_size, "offset": offset}
            data = self._post_graphql_fees(self.FEE_DEDUCTIONS_QUERY, variables)
            fees_node = data.get("feeDeductions", {})
            batch = fees_node.get("items", [])
            total_count = total_count or fees_node.get("totalCount")
            if not batch:
                logger.info("No more fee records after %d pages", page)
                break

            items.extend(batch)
            logger.info("Fetched fees page %d: %d items (offset=%d)", page + 1, len(batch), offset)

            if len(batch) < page_size:
                break

            offset += page_size
            if total_count and len(items) >= total_count:
                break

        logger.info(
            "Fetched %d fee records%s",
            len(items),
            f" out of {total_count}" if total_count else "",
        )
        return items

    def _upsert_monthly_summaries(self, session, booking_dates: set) -> None:
        """Update all summary tables (monthly, daily, product totals) for given booking dates.
        
        This maintains three aggregation levels:
        1. Monthly summaries (permanent, all history)
        2. Daily summaries (recent days for visualization)
        3. Product totals (lifetime totals per product/fee)
        """
        if not booking_dates:
            return
        try:
            unique_dates = {d for d in booking_dates if d}
            logger.info("_upsert_monthly_summaries: processing %d unique dates", len(unique_dates))
            for idx, bdate in enumerate(unique_dates, 1):
                logger.info("  Date %d/%d: %s - running DISTINCT ON query...", idx, len(unique_dates), bdate)
                month_key = bdate.strftime("%Y-%m") if hasattr(bdate, 'strftime') else str(bdate)[:7]

                # Aggregate data for this booking date.
                # Use the LATEST record per (product_isin, fee_type) for the day
                # instead of summing multiple intra-day records. This ensures
                # daily summaries reflect the most-recent state for each product.
                sql = text(
                    """
                    SELECT DISTINCT ON (product_isin, fee_type)
                        id, booking_date, product_isin, product_name, fee_type, fee_name, currency, amount_abs
                    FROM vestr_fee_records
                    WHERE booking_date = :bdate
                    ORDER BY product_isin, fee_type, updated_at DESC
                    """
                )

                rows = session.execute(sql, {'bdate': bdate}).fetchall()
                logger.info("  Date %d/%d: %s - found %d latest records, upserting summaries...", idx, len(unique_dates), bdate, len(rows))

                for row in rows:
                    # Access tuple by index: 0=id, 1=booking_date, 2=product_isin, 3=product_name, 4=fee_type, 5=fee_name, 6=currency, 7=amount_abs
                    prod_isin = row[2]
                    prod_name = row[3]
                    fee_type = row[4]
                    fee_name = row[5]
                    currency = row[6]
                    sum_amount = float(row[7] or 0.0)
                    rec_count = 1

                    # 1. Update MONTHLY summary
                    ins_month = insert(VestrFeeMonthlySummary).values(
                        month=month_key,
                        product_isin=prod_isin,
                        product_name=prod_name,
                        fee_type=fee_type,
                        fee_name=fee_name,
                        currency=currency,
                        sum_amount=sum_amount,
                        sum_abs=sum_amount,
                        record_count=rec_count,
                    )
                    ins_month = ins_month.on_conflict_do_update(
                        index_elements=['month', 'product_isin', 'fee_type'],
                        set_={
                            'sum_amount': VestrFeeMonthlySummary.sum_amount + ins_month.excluded.sum_amount,
                            'sum_abs': VestrFeeMonthlySummary.sum_abs + ins_month.excluded.sum_abs,
                            'record_count': VestrFeeMonthlySummary.record_count + ins_month.excluded.record_count,
                            'product_name': ins_month.excluded.product_name,
                            'fee_name': ins_month.excluded.fee_name,
                            'updated_at': datetime.utcnow(),
                        },
                    )
                    session.execute(ins_month)

                    # 2. Update DAILY summary (for recent day visualization)
                    ins_daily = insert(VestrFeeDailySummary).values(
                        booking_date=bdate,
                        product_isin=prod_isin,
                        product_name=prod_name,
                        fee_type=fee_type,
                        fee_name=fee_name,
                        currency=currency,
                        sum_amount=sum_amount,
                        sum_abs=sum_amount,
                        record_count=rec_count,
                    )
                    ins_daily = ins_daily.on_conflict_do_update(
                        index_elements=['booking_date', 'product_isin', 'fee_type'],
                        set_={
                            'sum_amount': ins_daily.excluded.sum_amount,  # Replace (not increment) for daily
                            'sum_abs': ins_daily.excluded.sum_abs,
                            'record_count': ins_daily.excluded.record_count,
                            'product_name': ins_daily.excluded.product_name,
                            'fee_name': ins_daily.excluded.fee_name,
                            'updated_at': datetime.utcnow(),
                        },
                    )
                    session.execute(ins_daily)

                    # 3. Update PRODUCT TOTAL (lifetime)
                    ins_prod = insert(VestrFeeProductTotal).values(
                        product_isin=prod_isin,
                        product_name=prod_name,
                        fee_type=fee_type,
                        currency=currency,
                        total_amount=sum_amount,
                        total_abs=sum_amount,
                        record_count=rec_count,
                        first_booking_date=bdate,
                        last_booking_date=bdate,
                    )
                    ins_prod = ins_prod.on_conflict_do_update(
                        index_elements=['product_isin', 'fee_type'],
                        set_={
                            'total_amount': VestrFeeProductTotal.total_amount + ins_prod.excluded.total_amount,
                            'total_abs': VestrFeeProductTotal.total_abs + ins_prod.excluded.total_abs,
                            'record_count': VestrFeeProductTotal.record_count + ins_prod.excluded.record_count,
                            'product_name': ins_prod.excluded.product_name,
                            'last_booking_date': func.greatest(VestrFeeProductTotal.last_booking_date, ins_prod.excluded.last_booking_date),
                            'first_booking_date': func.least(VestrFeeProductTotal.first_booking_date, ins_prod.excluded.first_booking_date),
                            'updated_at': datetime.utcnow(),
                        },
                    )
                    session.execute(ins_prod)
                    
        except Exception:
            logger.exception("Error while updating fee summary tables")

    def get_fees_overview(
        self,
        days: int = 365,
        fee_types: Optional[List[str]] = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        """Get comprehensive fees overview for dashboard.

        Returns summary stats, chart-ready monthly data, product insights,
        AMC coverage, and metadata about the cached dataset.
        """
        if fee_types is None:
            fee_types = [self.FEE_TYPE_MANAGEMENT, self.FEE_TYPE_PERFORMANCE]

        max_date = datetime.now().date()
        min_date = max_date - timedelta(days=days)

        try:
            items, cache_meta = self._load_fees_from_database(
                min_date=min_date,
                max_date=max_date,
                fee_types=fee_types,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            logger.warning("Fee database unavailable (%s). Falling back to disk cache.", exc)
            # Try disk cache before expensive remote fetch
            disk_entry = _load_disk_cache()
            if disk_entry:
                items, cache_meta = disk_entry
                logger.info("Using disk-cached fees (%d items)", len(items))
            else:
                logger.warning("No disk cache found, must fetch remotely")
                items, cache_meta = self._fetch_all_fees(force_refresh=True)

        product_totals: Dict[str, Dict[str, Any]] = {}
        monthly_totals: Dict[str, Dict[str, float]] = {}
        fee_type_totals: Dict[str, float] = {}
        currency_totals: Dict[str, Dict[str, float]] = {}
        fee_name_totals: Dict[str, Dict[str, float]] = {}
        amc_totals: Dict[str, Dict[str, Any]] = {}
        unique_amcs = set()
        total_amount = 0.0
        record_count = 0
        currencies_seen = set()
        recent_records: List[Dict[str, Any]] = []

        for item in items:
            fee_type = item.get("type", "")
            if fee_types and fee_type not in fee_types:
                continue

            booking_date = self._parse_date_value(item.get("bookingDate"))
            if not booking_date:
                continue

            row_date = booking_date.date()
            if row_date < min_date or row_date > max_date:
                continue

            product = item.get("product", {}) or {}
            product_name = product.get("name", "Unknown")
            isin = product.get("isin", "")
            amount = abs(float(item.get("positionChange", 0) or 0))
            currency = item.get("currency", "EUR")
            fee_name_value = item.get("feeName") or fee_type.replace("FeeDeduction", " Fee")
            beneficiary_id = item.get("beneficiaryId")
            raw_amc_units = item.get("outstandingQuantity")
            amc_units = None
            if raw_amc_units is not None:
                try:
                    amc_units = float(str(raw_amc_units).replace(",", ""))
                except (TypeError, ValueError):
                    amc_units = None

            currencies_seen.add(currency)
            total_amount += amount
            record_count += 1

            if product_name not in product_totals:
                product_totals[product_name] = {
                    "product_name": product_name,
                    "isin": isin,
                    "total": 0.0,
                    "count": 0,
                    "management": 0.0,
                    "performance": 0.0,
                    "amc_ids": set(),
                    "last_fee_date": None,
                }

            prod_entry = product_totals[product_name]
            prod_entry["total"] += amount
            prod_entry["count"] += 1
            if fee_type == self.FEE_TYPE_MANAGEMENT:
                prod_entry["management"] += amount
            elif fee_type == self.FEE_TYPE_PERFORMANCE:
                prod_entry["performance"] += amount
            
            # Track latest fee date (any type)
            if prod_entry["last_fee_date"] is None or row_date > prod_entry["last_fee_date"]:
                prod_entry["last_fee_date"] = row_date
            
            if beneficiary_id:
                prod_entry["amc_ids"].add(beneficiary_id)

            month_key = row_date.strftime("%Y-%m")
            if month_key not in monthly_totals:
                monthly_totals[month_key] = {
                    self.FEE_TYPE_MANAGEMENT: 0.0,
                    self.FEE_TYPE_PERFORMANCE: 0.0,
                }
            monthly_totals[month_key][fee_type] = monthly_totals[month_key].get(fee_type, 0.0) + amount

            fee_type_totals[fee_type] = fee_type_totals.get(fee_type, 0.0) + amount

            currency_entry = currency_totals.setdefault(currency, {"total": 0.0, "records": 0})
            currency_entry["total"] += amount
            currency_entry["records"] += 1

            fee_name_entry = fee_name_totals.setdefault(fee_name_value, {"total": 0.0, "records": 0})
            fee_name_entry["total"] += amount
            fee_name_entry["records"] += 1

            if beneficiary_id:
                unique_amcs.add(beneficiary_id)
                amc_entry = amc_totals.setdefault(beneficiary_id, {"total": 0.0, "records": 0, "products": set()})
                amc_entry["total"] += amount
                amc_entry["records"] += 1
                amc_entry["products"].add(product_name)

            if amc_units is not None:
                last_units_date = prod_entry.get("amc_units_date")
                if last_units_date is None or row_date >= last_units_date:
                    prod_entry["amc_units"] = amc_units
                    prod_entry["amc_units_date"] = row_date

            recent_records.append(
                {
                    "date": booking_date,
                    "row_date": row_date,
                    "product_name": product_name,
                    "fee_type": fee_type,
                    "fee_name": fee_name_value,
                    "amount": amount,
                    "currency": currency,
                    "beneficiary_id": beneficiary_id,
                    "amc_units": amc_units,
                }
            )

        for product in product_totals.values():
            amc_ids = product.pop("amc_ids", set())
            product["amc_count"] = len(amc_ids)
            product["amc_units"] = product.pop("amc_units", None)
            product.pop("amc_units_date", None)
            # Format last fee date
            last_fee_date_val = product.get("last_fee_date")
            if last_fee_date_val:
                product["last_fee_date"] = last_fee_date_val.strftime("%Y-%m-%d")
            else:
                product["last_fee_date"] = None

        all_products_sorted = sorted(
            product_totals.values(),
            key=lambda x: x["total"],
            reverse=True,
        )
        sorted_products = all_products_sorted[:5]

        month_keys = _month_key_iter(min_date, max_date)
        month_labels: List[str] = []
        for key in month_keys:
            try:
                dt = datetime.strptime(key + "-01", "%Y-%m-%d")
                month_labels.append(dt.strftime("%b %Y"))
            except Exception:
                month_labels.append(key)

        monthly_chart = {
            "labels": month_labels,
            "management": [monthly_totals.get(k, {}).get(self.FEE_TYPE_MANAGEMENT, 0.0) for k in month_keys],
            "performance": [monthly_totals.get(k, {}).get(self.FEE_TYPE_PERFORMANCE, 0.0) for k in month_keys],
        }

        management_total = fee_type_totals.get(self.FEE_TYPE_MANAGEMENT, 0.0)
        performance_total = fee_type_totals.get(self.FEE_TYPE_PERFORMANCE, 0.0)

        amc_entries = [
            {
                "beneficiary_id": amc_id,
                "total": stats["total"],
                "records": stats["records"],
                "products": len(stats["products"]),
            }
            for amc_id, stats in amc_totals.items()
        ]
        amc_entries.sort(key=lambda x: x["total"], reverse=True)
        amc_summary = {
            "unique_amcs": len(unique_amcs),
            "top_amcs": amc_entries[:10],
        }

        currency_breakdown = sorted(
            (
                {"currency": cur, "total": stats["total"], "records": stats["records"]}
                for cur, stats in currency_totals.items()
            ),
            key=lambda x: x["total"],
            reverse=True,
        )

        fee_name_breakdown = sorted(
            (
                {"fee_name": name, "total": stats["total"], "records": stats["records"]}
                for name, stats in fee_name_totals.items()
            ),
            key=lambda x: x["total"],
            reverse=True,
        )

        recent_fees: List[Dict[str, Any]] = []
        recent_rows, recent_target_date = _select_recent_fee_rows(recent_records)
        for record in recent_rows:
            beneficiary_id = record.get("beneficiary_id")
            amc_display = beneficiary_id if beneficiary_id else "—"
            amc_units = record.get("amc_units")
            recent_fees.append(
                {
                    "date": record["row_date"].strftime("%Y-%m-%d") if record.get("row_date") else record["date"].strftime("%Y-%m-%d"),
                    "product_name": record["product_name"],
                    "fee_type": record["fee_type"],
                    "fee_name": record["fee_name"],
                    "amount": record["amount"],
                    "currency": record["currency"],
                    "amc_id": amc_display,
                    "amc_units": amc_units,
                }
            )

        # Calculate years span for dynamic All Time label
        years_span = round((max_date - min_date).days / 365.25, 1)
        # Provide palette hints and a human-friendly timespan label so the
        # frontend can display consistent site colors and correct range text.
        # We include both CSS variable names and default hex fallbacks.
        monthly_chart["colors"] = {
            "management": "#33455d",
            "performance": "#e9d99b",
            "css_vars": {"management": "--primary-ais", "performance": "--secondary-ais"},
        }

        # Compute a readable timespan label. If the requested window is >= 1 year
        # prefer an "All Time (X years)" style label, otherwise show last N months.
        timespan_label = None
        try:
            if days >= 365:
                timespan_label = f"All Time ({years_span} years)"
            else:
                months = max(1, int(round(days / 30.0)))
                timespan_label = "Last Month" if months == 1 else f"Last {months} months"
        except Exception:
            timespan_label = None

        meta = {
            "record_count": cache_meta.get("record_count", len(items)),
            "last_refreshed": _format_iso_datetime(cache_meta.get("fetched_at")) if cache_meta.get("fetched_at") else None,
            "source": cache_meta.get("source", "remote"),
            "sync_mode": cache_meta.get("run_mode"),
            "sync_status": cache_meta.get("status"),
            "requested_days": days,
            "cache_hours": FEES_CACHE_MAX_AGE.total_seconds() / 3600,
            "years_span": years_span,
            "recent_activity_date": recent_target_date.strftime("%Y-%m-%d") if recent_target_date else None,
            "timespan_label": timespan_label,
        }

        return {
            "summary": {
                "total_fees": total_amount,
                "total_records": record_count,
                "total_products": len(product_totals),
                "management_fees": management_total,
                "performance_fees": performance_total,
                "currency": list(currencies_seen)[0] if len(currencies_seen) == 1 else "Mixed",
                "unique_amcs": len(unique_amcs),
                "avg_ticket": total_amount / record_count if record_count else 0.0,
            },
            "date_range": {
                "from": min_date.strftime("%Y-%m-%d"),
                "to": max_date.strftime("%Y-%m-%d"),
                "days": days,
            },
            "top_products": sorted_products,
            "all_products": all_products_sorted,
            "monthly_chart": monthly_chart,
            "fee_distribution": {
                "management": management_total,
                "performance": performance_total,
                "management_pct": round(100 * management_total / total_amount, 1) if total_amount > 0 else 0,
                "performance_pct": round(100 * performance_total / total_amount, 1) if total_amount > 0 else 0,
            },
            "amc_summary": amc_summary,
            "currency_breakdown": currency_breakdown,
            "fee_names": fee_name_breakdown,
            "recent_fees": recent_fees,
            "meta": meta,
        }

    def download_fees(
        self,
        download_dir: str,
        min_group_date: Optional[datetime] = None,
        fee_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Download fee records to CSV."""
        target_date = self._as_date(min_group_date or self.DEFAULT_MIN_GROUP_DATE)
        os.makedirs(download_dir, exist_ok=True)
        
        if fee_types is None:
            fee_types = [self.FEE_TYPE_MANAGEMENT, self.FEE_TYPE_PERFORMANCE]

        items: List[Dict[str, Any]] = []
        try:
            items, _ = self._load_fees_from_database(
                min_date=target_date or self.DEFAULT_MIN_GROUP_DATE.date(),
                max_date=datetime.utcnow().date(),
                fee_types=fee_types,
                force_refresh=False,
            )
        except Exception as exc:
            logger.warning("Fee database unavailable for CSV download (%s). Fetching live data.", exc)
            self.login()
            items, _ = self._fetch_all_fees(force_refresh=True)
        
        rows = []
        for item in items:
            fee_type = item.get("type", "")
            if fee_types and fee_type not in fee_types:
                continue
            
            booking_date = self._parse_date_value(item.get("bookingDate"))
            if booking_date and target_date and booking_date.date() < target_date:
                continue
            
            product = item.get("product", {}) or {}
            rows.append({
                "Date": booking_date.strftime("%d.%m.%Y") if booking_date else "",
                "Product": product.get("name", ""),
                "ISIN": product.get("isin", ""),
                "Fee Type": fee_type.replace("FeeDeduction", " Fee"),
                "Amount": item.get("positionChange", ""),
                "Currency": item.get("currency", ""),
            })
        
        # Sort by date descending
        rows.sort(key=lambda r: r.get("Date", ""), reverse=True)
        
        csv_text = self._records_to_csv(rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(download_dir, f"fees_{timestamp}.csv")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(csv_text)

        return {
            "success": True,
            "file_path": file_path,
            "rows": len(rows),
            "message": f"Fees CSV downloaded ({len(rows)} records)",
        }

    def _parse_date_value(self, raw: Any) -> Optional[datetime]:
        """Parse date from various formats."""
        if raw in (None, ""):
            return None
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, date):
            return datetime.combine(raw, datetime.min.time())
        if isinstance(raw, (int, float)):
            try:
                value = float(raw)
                if value > 1e11:
                    value /= 1000.0
                return datetime.fromtimestamp(value)
            except:
                return None
        if isinstance(raw, str):
            cleaned = raw.strip()
            if not cleaned:
                return None
            iso_candidate = cleaned.replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(iso_candidate)
            except:
                pass
            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(cleaned[:10], fmt)
                except:
                    continue
        return None

    def _as_date(self, value: Optional[datetime]) -> Optional[date]:
        """Convert datetime to date."""
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        return value.date()

    def _records_to_csv(self, records: List[Dict[str, Any]]) -> str:
        """Convert records to CSV string."""
        if not records:
            return ""
        fieldnames = list(records[0].keys())
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
        return output.getvalue()


def get_fees_overview(
    days: int = 365,
    fee_types: Optional[List[str]] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """Public helper to get fees overview for dashboard."""
    scraper = LightweightVestrFeesScraper()
    return scraper.get_fees_overview(days=days, fee_types=fee_types, force_refresh=force_refresh)


def download_fees_csv(
    download_dir: Optional[str] = None,
    min_group_date: Optional[datetime] = None,
    fee_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Public helper to download fees CSV."""
    download_dir = download_dir or os.path.join(os.getcwd(), "uploads")
    os.makedirs(download_dir, exist_ok=True)
    
    scraper = LightweightVestrFeesScraper()
    return scraper.download_fees(download_dir, min_group_date=min_group_date, fee_types=fee_types)


def sync_fees_dataset(force_full: bool = False) -> Dict[str, Any]:
    """Trigger a background synchronization of Vestr fees into the database."""
    scraper = LightweightVestrFeesScraper()
    scraper.login()
    return scraper._sync_database_with_remote(full_refresh=force_full)

