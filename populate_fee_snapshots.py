"""
Populate fee_latest_snapshot table using incremental updates.
Only refresh products whose latest booking date advanced since the last run.
"""
from datetime import datetime
from typing import Optional, Tuple

from sqlalchemy import func

from database_models import get_session, VestrFeeRecord, FeeLatestSnapshot


def _normalize_product_key(product_isin: Optional[str], product_name: Optional[str]) -> Optional[str]:
    if product_isin and product_isin.strip():
        return product_isin.strip()
    if not product_name:
        return None
    sanitized = ''.join(ch if ch.isalnum() else '_' for ch in product_name.upper())
    key = f"UNKNOWN_{sanitized}"[:32]
    return key or None


def _build_snapshot_payload(
    session,
    snapshot_isin: str,
    raw_isin: Optional[str],
    product_name: Optional[str]
) -> Optional[dict]:
    filters = []
    if raw_isin:
        filters.append(VestrFeeRecord.product_isin == raw_isin)
    elif product_name:
        filters.append(VestrFeeRecord.product_name == product_name)
    else:
        return None

    base_filters = tuple(filters)
    ordering = (
        VestrFeeRecord.booking_date.desc(),
        VestrFeeRecord.booking_datetime.desc(),
        VestrFeeRecord.updated_at.desc()
    )

    base_query = session.query(VestrFeeRecord).filter(*base_filters)
    latest_overall = base_query.order_by(*ordering).first()
    if not latest_overall:
        return None

    def latest_of_type(fee_type: str):
        return session.query(VestrFeeRecord).filter(
            *base_filters,
            VestrFeeRecord.fee_type == fee_type
        ).order_by(*ordering).first()

    latest_mgmt = latest_of_type('ManagementFeeDeduction')
    latest_perf = latest_of_type('PerformanceFeeDeduction')
    latest_custody = latest_of_type('CustodyFeeDeduction')

    return {
        'product_isin': snapshot_isin,
        'product_name': latest_overall.product_name or product_name,
        'last_mgmt_fee_date': latest_mgmt.booking_date if latest_mgmt else None,
        'last_mgmt_fee_amount': abs(latest_mgmt.position_change or 0.0) if latest_mgmt else None,
        'last_perf_fee_date': latest_perf.booking_date if latest_perf else None,
        'last_perf_fee_amount': abs(latest_perf.position_change or 0.0) if latest_perf else None,
        'last_custody_fee_date': latest_custody.booking_date if latest_custody else None,
        'last_custody_fee_amount': abs(latest_custody.position_change or 0.0) if latest_custody else None,
        'last_fee_date': latest_overall.booking_date,
        'last_fee_type': latest_overall.fee_type,
        'last_fee_amount': abs(latest_overall.position_change or 0.0),
        'currency': latest_overall.currency,
        'outstanding_quantity': latest_overall.outstanding_quantity,
    }


def populate_snapshots():
    """Incrementally update fee_latest_snapshot with only the newest data."""
    print("=" * 80)
    print("POPULATE FEE SNAPSHOTS (Incremental)")
    print("=" * 80)

    session = get_session()
    try:
        raw_count = session.query(VestrFeeRecord).count()
        snapshot_count_before = session.query(FeeLatestSnapshot).count()

        print(f"\n📊 Current State:")
        print(f"   Raw records: {raw_count}")
        print(f"   Snapshot records (before): {snapshot_count_before}")

        if raw_count == 0:
            print("\n⚠️  No raw records available - cannot populate snapshots")
            return

        latest_rows = session.query(
            VestrFeeRecord.product_name,
            VestrFeeRecord.product_isin,
            func.max(VestrFeeRecord.booking_date).label('latest_date')
        ).group_by(
            VestrFeeRecord.product_name,
            VestrFeeRecord.product_isin
        ).all()

        latest_by_key = {}
        metadata_by_key = {}
        for product_name, raw_isin, latest_date in latest_rows:
            normalized = _normalize_product_key(raw_isin, product_name)
            if not normalized or not latest_date:
                continue
            stored = latest_by_key.get(normalized)
            if stored is None or latest_date > stored:
                latest_by_key[normalized] = latest_date
                metadata_by_key[normalized] = (raw_isin, product_name)

        if not latest_by_key:
            print("\n⚠️  No products with valid identifiers were found")
            return

        existing_dates = dict(
            session.query(
                FeeLatestSnapshot.product_isin,
                FeeLatestSnapshot.last_fee_date
            ).all()
        )

        targets = []
        for key, latest_date in latest_by_key.items():
            current_date = existing_dates.get(key)
            if current_date is None or latest_date > current_date:
                targets.append((key, *metadata_by_key[key]))

        print(f"\n🔁 Products needing refresh: {len(targets)}")
        if not targets:
            print("   Snapshots already reflect the most recent data.")
            return

        created = 0
        updated = 0
        for snapshot_key, raw_isin, product_name in targets:
            payload = _build_snapshot_payload(session, snapshot_key, raw_isin, product_name)
            if not payload:
                continue

            snapshot = session.query(FeeLatestSnapshot).filter(
                FeeLatestSnapshot.product_isin == snapshot_key
            ).one_or_none()

            if snapshot is None:
                snapshot = FeeLatestSnapshot(product_isin=snapshot_key)
                session.add(snapshot)
                created += 1
            else:
                updated += 1

            snapshot.product_name = payload['product_name']
            snapshot.last_mgmt_fee_date = payload['last_mgmt_fee_date']
            snapshot.last_mgmt_fee_amount = payload['last_mgmt_fee_amount']
            snapshot.last_perf_fee_date = payload['last_perf_fee_date']
            snapshot.last_perf_fee_amount = payload['last_perf_fee_amount']
            snapshot.last_custody_fee_date = payload['last_custody_fee_date']
            snapshot.last_custody_fee_amount = payload['last_custody_fee_amount']
            snapshot.last_fee_date = payload['last_fee_date']
            snapshot.last_fee_type = payload['last_fee_type']
            snapshot.last_fee_amount = payload['last_fee_amount']
            snapshot.currency = payload['currency']
            snapshot.outstanding_quantity = payload['outstanding_quantity']
            snapshot.synced_at = datetime.utcnow()

        session.commit()

        snapshot_count_after = session.query(FeeLatestSnapshot).count()

        print(f"\n✅ Snapshot changes applied:")
        print(f"   Inserted: {created}")
        print(f"   Updated: {updated}")
        print(f"   Total snapshot records: {snapshot_count_after}")

        print(f"\n📋 Sample Snapshots (first 5):")
        samples = session.query(FeeLatestSnapshot).limit(5).all()
        for s in samples:
            print(
                f"   {s.product_name[:30]:30} | Units: {s.outstanding_quantity or 0:>10.2f} | Last: {s.last_fee_date}"
            )

        print("\n" + "=" * 80)
        print("COMPLETE")
        print("=" * 80)

    except Exception as e:
        session.rollback()
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


if __name__ == '__main__':
    populate_snapshots()
