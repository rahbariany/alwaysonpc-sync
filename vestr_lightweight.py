"""
Lightweight Vestr scraper using requests + BeautifulSoup
Memory usage: ~50MB (vs 566MB with Selenium)
"""
import csv
import io
import json
import logging
import os
import re
import time
import requests
import pyotp
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

class LightweightVestrScraper:
    """Scrape Vestr using direct HTTP requests (no browser)"""

    GRAPHQL_URL = "https://aisfg.delta.vestr.com/graphql"
    PAGINATED_NAV_QUERY = """
    query PaginatedNavProductList(
        $filter: ProductFilter
        $limit: Int!
        $offset: Int
        $sortBy: SortBy
        $startDateTime: DateTime
        $endDateTime: DateTime
        $withSnapshotNetAssetValue: Boolean!
        $withTentativeNetAssetValue: Boolean!
        $withExternalNetAssetValue: Boolean!
    ) {
        paginatedProducts(filter: $filter, limit: $limit, offset: $offset, sortBy: $sortBy) {
            items {
                id
                isin
                currency
                quotingType
                managementStyle
                name
                mainPortfolioManagerOrganizationName
                portfolio
                externalTentativeNetAssetValue @include(if: $withExternalNetAssetValue)
                tentativeNetAssetValue @include(if: $withTentativeNetAssetValue)
                importSnapshotNetAssetValue @include(if: $withSnapshotNetAssetValue)
                issuerName
                report {
                    timeSeriesDailyMidPrices(
                        startDateTime: $startDateTime
                        endDateTime: $endDateTime
                        selectWeekDays: [1, 2, 3, 4, 5]
                    ) {
                        dateTime
                        price
                        productId
                    }
                }
            }
            totalCount
        }
    }
    """
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Hardcoded credentials
        self.username = "crudi"
        self.password = "Minimarseillais1#"
        self.otp_secret = "KRRHIUCNGRBHOZLRHBBVAUSDGZJUC4SM"
    
    def login(self):
        """Login to Vestr using Keycloak"""
        logger.info("[LOGIN] Starting lightweight login...")
        
        # Step 1: Get login page
        login_url = "https://aisfg.delta.vestr.com"
        resp = self.session.get(login_url, allow_redirects=True)
        logger.info(f"Login page loaded: {resp.status_code}")
        
        # Extract form action URL from Keycloak
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, 'html.parser')
        form = soup.find('form', {'id': 'kc-form-login'})
        if not form:
            raise Exception("Login form not found")
        
        action_url = urljoin(resp.url, form.get('action') or '')
        logger.info(f"Form action: {action_url}")
        
        # Step 2: Submit username + password (including hidden PKCE fields)
        login_data = self._collect_form_fields(form)
        login_data.update({
            'username': self.username,
            'password': self.password
        })
        resp = self.session.post(action_url, data=login_data, allow_redirects=True)
        logger.info(f"Password submitted: {resp.status_code}")
        
        # Step 3: Handle OTP with retries (Keycloak occasionally rejects stale codes)
        soup = BeautifulSoup(resp.text, 'html.parser')
        otp_form = soup.find('form', {'id': 'kc-otp-login-form'})
        if not otp_form:
            logger.warning("No OTP form found - checking if already logged in")
            if "products-admin" in resp.url:
                logger.info("[SUCCESS] Already logged in!")
                return True
            raise Exception("OTP form not found and not logged in")

        otp_verified = False
        max_otp_attempts = 5

        for attempt in range(1, max_otp_attempts + 1):
            totp = pyotp.TOTP(self.otp_secret)
            otp_code = totp.now()
            logger.info(f"Generated OTP (attempt {attempt}): {otp_code}")

            otp_action = urljoin(resp.url, otp_form.get('action') or '')
            otp_data = self._collect_form_fields(otp_form)
            otp_data.update({'otp': otp_code})

            resp = self.session.post(otp_action, data=otp_data, allow_redirects=True)
            logger.info(f"OTP submitted (attempt {attempt}): {resp.status_code}, URL: {resp.url}")

            soup = BeautifulSoup(resp.text, 'html.parser')
            otp_form = soup.find('form', {'id': 'kc-otp-login-form'})
            if not otp_form:
                otp_verified = True
                break

            feedback = soup.find(class_='kc-feedback-text')
            if feedback:
                logger.warning(f"OTP challenge still active (attempt {attempt}): {feedback.get_text(strip=True)}")
            else:
                logger.warning(f"OTP challenge still active after attempt {attempt}; retrying with fresh code...")
            time.sleep(2)

        if not otp_verified:
            raise Exception("OTP verification failed after multiple attempts")
        
        # Keycloak redirects back to app - follow any hidden auto forms to finish hand-off
        resp = self._submit_auto_forms(resp)
        
        # Verify we're logged in by checking for products-admin in URL or content
        if "products-admin" in resp.url or "products-admin" in resp.text[:500]:
            logger.info("[SUCCESS] Login successful!")
            return True
        
        # Try one more time - directly access products-admin
        logger.info("Trying direct access to products-admin...")
        resp = self.session.get("https://aisfg.delta.vestr.com/products-admin/", allow_redirects=True)
        
        if "auth" not in resp.url and "login" not in resp.url:
            logger.info("[SUCCESS] Login successful (via direct access)!")
            return True
        
        raise Exception(f"Login failed. Final URL: {resp.url}")

    def _submit_auto_forms(self, resp, max_rounds=5):
        """Follow hidden OAuth auto-submit forms until session established"""
        from bs4 import BeautifulSoup
        round_idx = 0
        while round_idx < max_rounds:
            soup = BeautifulSoup(resp.text, 'html.parser')
            auto_form = self._pick_auto_form(soup)
            if not auto_form:
                break
            action = auto_form.get('action') or ''
            full_action = urljoin(resp.url, action)
            method = (auto_form.get('method') or 'post').lower()
            payload = self._collect_form_fields(auto_form)
            round_idx += 1
            logger.info(f"Submitting auto-form round {round_idx} -> {full_action}")
            if method == 'get':
                resp = self.session.get(full_action, params=payload, allow_redirects=True)
            else:
                resp = self.session.post(full_action, data=payload, allow_redirects=True)
            logger.info(f"Auto-form round {round_idx} result: {resp.status_code}, URL: {resp.url}")
            if "products-admin" in resp.url:
                break
        return resp

    @staticmethod
    def _pick_auto_form(soup):
        """Choose the hidden OAuth form (skip login/password/otp forms)"""
        for form in soup.find_all('form'):
            inputs = form.find_all('input')
            names = {inp.get('name', '').lower() for inp in inputs}
            if not inputs:
                continue
            if {'username', 'password', 'otp'} & names:
                continue
            if form.get('id') in {'kc-form-login', 'kc-otp-login-form'}:
                continue
            return form
        return None

    @staticmethod
    def _collect_form_fields(form):
        """Collect all input values from a form (including hidden PKCE fields)"""
        payload = {}
        for input_field in form.find_all('input'):
            name = input_field.get('name')
            if not name:
                continue
            value = input_field.get('value', '')
            payload[name] = value
        return payload
    
    def download_csv(self, download_dir):
        """Download NAV CSV using Vestr API"""
        logger.info("📥 Downloading CSV via API...")
        
        # Navigate to NAVs page first (establishes session)
        try:
            nav_page = self.session.get("https://aisfg.delta.vestr.com/products-admin/navs", timeout=15)
            logger.info(f"NAVs page: {nav_page.status_code}")
            
            # Check if we got redirected (not logged in properly)
            if "auth" in nav_page.url or "login" in nav_page.url:
                raise Exception("Session not authenticated - redirected to login")
        except Exception as e:
            logger.error(f"Failed to access NAVs page: {e}")
            raise Exception(f"Cannot access NAVs page: {e}")
        
        csv_text = None
        try:
            csv_text = self._download_navs_via_graphql()
            if csv_text:
                logger.info("✅ NAV data captured via GraphQL endpoint")
        except Exception as gql_error:
            logger.warning(f"GraphQL NAV query failed: {gql_error}")

        if csv_text:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = os.path.join(download_dir, f"vestr_navs_{timestamp}.csv")
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(csv_text)
            return {'success': True, 'file_path': filepath, 'message': 'CSV downloaded via GraphQL NAV query'}

        # Try legacy export endpoints as fallback
        candidate_urls = self._discover_candidate_api_urls(nav_page.text)
        candidate_urls.extend([
            "https://aisfg.delta.vestr.com/products-admin/api/navs/export",
            "https://aisfg.delta.vestr.com/products-admin/api/navs/csv",
            "https://aisfg.delta.vestr.com/api/navs",
            "https://aisfg.delta.vestr.com/products-admin/api/navs/list",
            "https://aisfg.delta.vestr.com/products-admin/api/navs/grid",
            "https://aisfg.delta.vestr.com/api/navs/export",
            "https://aisfg.delta.vestr.com/api/export/navs",
        ])
        candidate_urls = list(dict.fromkeys(candidate_urls))  # dedupe while preserving order

        for url in candidate_urls:
            try:
                csv_text = self._try_endpoint_for_csv(url)
                if csv_text:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"vestr_navs_{timestamp}.csv"
                    filepath = os.path.join(download_dir, filename)
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(csv_text)
                    logger.info(f"✅ CSV saved from {url}: {filepath} ({len(csv_text)} bytes)")
                    return {'success': True, 'file_path': filepath, 'message': f'CSV downloaded via {url}'}
            except Exception as e:
                logger.warning(f"  Endpoint failed: {url} -> {e}")
                continue

        # No API endpoint worked
        logger.error("❌ No API endpoint worked. Need to use Selenium.")
        raise Exception("Lightweight scraper cannot find CSV export API. Will use Selenium fallback.")

    def _download_navs_via_graphql(self):
        csrf_token = self.session.cookies.get('csrf-token')
        if not csrf_token:
            raise Exception("Missing CSRF token for GraphQL call")

        start_iso, end_iso = self._compute_nav_window(business_days=10)
        limit = 200
        offset = 0
        items = []
        headers = {'x-csrf-token': csrf_token}
        variables = {
            'filter': None,
            'sortBy': None,
            'startDateTime': start_iso,
            'endDateTime': end_iso,
            'withSnapshotNetAssetValue': True,
            'withTentativeNetAssetValue': True,
            'withExternalNetAssetValue': True,
        }

        while True:
            variables['limit'] = limit
            variables['offset'] = offset
            page = self._post_graphql(self.PAGINATED_NAV_QUERY, variables, headers)
            page_items = page.get('items', [])
            if not page_items:
                break
            items.extend(page_items)
            total_count = page.get('totalCount', len(items))
            if len(items) >= total_count:
                break
            offset += limit

        if not items:
            raise Exception("GraphQL response contained no NAV rows")

        window_days = self._derive_reporting_window(items, column_count=5)
        records = self._nav_items_to_records(items, window_days)
        fieldnames = self._build_fieldnames(window_days)
        return self._records_to_csv(records, fieldnames=fieldnames)

    def _post_graphql(self, query, variables, headers):
        resp = self.session.post(
            self.GRAPHQL_URL,
            json={'query': query, 'variables': variables, 'operationName': 'PaginatedNavProductList'},
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get('errors'):
            raise Exception(payload['errors'])
        data = payload.get('data', {})
        products = data.get('paginatedProducts')
        if not products:
            raise Exception('GraphQL payload missing paginatedProducts')
        return products

    def _compute_nav_window(self, business_days=10):
        days = []
        cursor = datetime.utcnow().date()
        while len(days) < business_days:
            if cursor.weekday() < 5:
                days.append(cursor)
            cursor -= timedelta(days=1)
        days.sort()
        start = datetime.combine(days[0], datetime.min.time()).isoformat() + 'Z'
        end = datetime.combine(days[-1], datetime.max.time()).isoformat() + 'Z'
        return start, end

    def _derive_reporting_window(self, items, column_count=5):
        all_dates = set()
        for item in items:
            series = ((item or {}).get('report') or {}).get('timeSeriesDailyMidPrices') or []
            for entry in series:
                date_obj = self._parse_series_date(entry.get('dateTime'))
                if date_obj:
                    all_dates.add(date_obj)

        if not all_dates:
            return self._recent_business_days(column_count)

        latest_date = max(all_dates)
        return self._recent_business_days(column_count, anchor=latest_date)

    def _recent_business_days(self, count, anchor=None):
        anchor = anchor or datetime.utcnow().date()
        days = []
        cursor = anchor
        safety = 0
        while len(days) < count and safety < 90:
            if cursor.weekday() < 5:
                days.append(cursor)
            cursor -= timedelta(days=1)
            safety += 1
        return list(reversed(days))

    def _build_fieldnames(self, window_days):
        date_headers = [self._format_business_day_label(day) for day in window_days]
        base_headers = [
            'Suggested NAV',
            'Suggested vs. latest NAV',
            'Product',
            'Organization',
            'ISIN',
            'Portfolio',
            'Snapshot NAV',
            'Latest NAV',
            'Snapshot vs. latest NAV',
            'Latest NAV move',
        ]
        return base_headers + date_headers

    def _format_business_day_label(self, day):
        return day.strftime('%a %d.%m.%Y')

    def _nav_items_to_records(self, items, window_days):
        records = []
        date_headers = [self._format_business_day_label(day) for day in window_days]

        for item in items:
            series_entries = self._normalize_series_entries(item)
            latest_entry = series_entries[-1] if series_entries else None
            previous_entry = series_entries[-2] if len(series_entries) > 1 else None

            suggested_nav = self._format_amount(item.get('tentativeNetAssetValue'))
            snapshot_nav = self._format_amount(item.get('importSnapshotNetAssetValue'))
            latest_nav = (latest_entry or {}).get('amount', '')
            previous_nav = (previous_entry or {}).get('amount', '')

            record = {
                'Suggested NAV': suggested_nav,
                'Suggested vs. latest NAV': self._relative_diff(suggested_nav, latest_nav),
                'Product': item.get('name') or '',
                'Organization': item.get('mainPortfolioManagerOrganizationName') or item.get('issuerName') or '',
                'ISIN': item.get('isin') or '',
                'Portfolio': item.get('portfolio') or '',
                'Snapshot NAV': snapshot_nav,
                'Latest NAV': latest_nav,
                'Snapshot vs. latest NAV': self._relative_diff(snapshot_nav, latest_nav),
                'Latest NAV move': self._relative_move(latest_nav, previous_nav),
            }

            # Map series values to requested weekday headers
            day_price_map = {entry['date']: entry['amount'] for entry in series_entries}
            for header, day in zip(date_headers, window_days):
                record[header] = day_price_map.get(day, '')

            records.append(record)

        return records

    def _normalize_series_entries(self, item):
        report = (item or {}).get('report') or {}
        series = report.get('timeSeriesDailyMidPrices') or []
        normalized = []
        for entry in series:
            date_obj = self._parse_series_date(entry.get('dateTime'))
            if not date_obj:
                continue
            normalized.append({
                'date': date_obj,
                'amount': self._format_amount(entry.get('price')),
            })
        normalized.sort(key=lambda row: row['date'])
        return normalized

    @staticmethod
    def _parse_series_date(raw_value):
        if not raw_value:
            return None
        try:
            normalized = raw_value.replace('Z', '+00:00')
            return datetime.fromisoformat(normalized).date()
        except ValueError:
            try:
                return datetime.strptime(raw_value[:10], '%Y-%m-%d').date()
            except Exception:
                return None

    def _format_amount(self, price_obj):
        if not price_obj:
            return ''
        if isinstance(price_obj, dict):
            amount = price_obj.get('displayAmount') or price_obj.get('amount')
        else:
            amount = price_obj
        if amount is None:
            return ''
        return self._stringify(amount)

    def _relative_diff(self, value, baseline):
        value_dec = self._to_decimal(value)
        baseline_dec = self._to_decimal(baseline)
        if value_dec is None or baseline_dec in (None, Decimal('0')):
            return ''
        return self._stringify((value_dec - baseline_dec) / baseline_dec)

    def _relative_move(self, current_value, previous_value):
        current_dec = self._to_decimal(current_value)
        previous_dec = self._to_decimal(previous_value)
        if current_dec in (None, Decimal('0')) or previous_dec is None:
            return ''
        return self._stringify((current_dec - previous_dec) / current_dec)

    @staticmethod
    def _to_decimal(value):
        if value in (None, ''):
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None

    def _discover_candidate_api_urls(self, html_text):
        """Scan page HTML for possible API endpoints"""
        candidates = []
        matches = re.findall(r"https?:\\/\\/[^\"']+/api/[A-Za-z0-9_\-/]+", html_text)
        matches += [f"https://aisfg.delta.vestr.com{path}" for path in re.findall(r"/products-admin/api/[A-Za-z0-9_\-/]+", html_text)]
        for url in matches:
            if 'logout' in url.lower():
                continue
            if 'auth' in url.lower():
                continue
            if any(token in url for token in ['nav', 'export', 'grid', 'report']):
                candidates.append(url)
        logger.info(f"Discovered {len(candidates)} potential API endpoints from HTML")
        return candidates

    def _try_endpoint_for_csv(self, url):
        """Attempt to fetch CSV or JSON rows from an endpoint"""
        logger.info(f"Trying endpoint: {url}")
        resp = self.session.get(url, timeout=20)
        logger.info(f"  GET status: {resp.status_code}, Content-Type: {resp.headers.get('content-type')}")
        csv_text = self._extract_csv_text(resp)
        if csv_text:
            return csv_text

        json_records = self._extract_json_records(resp)
        if json_records:
            logger.info(f"  JSON payload detected with {len(json_records)} rows")
            return self._records_to_csv(json_records)

        if resp.status_code in (401, 403):
            raise Exception("Session lost during API call")

        # Try POST fallbacks for ag-grid style endpoints
        post_payloads = [
            {
                "startRow": 0,
                "endRow": 2000,
                "rowGroupCols": [],
                "valueCols": [],
                "pivotCols": [],
                "pivotMode": False,
                "groupKeys": [],
                "filterModel": {},
                "sortModel": [],
            },
            {"page": 0, "size": 2000, "sort": "date,desc"},
            {"start": 0, "length": 2000},
        ]

        for payload in post_payloads:
            try:
                logger.info(f"  POST attempt with payload keys: {list(payload.keys())}")
                resp = self.session.post(url, json=payload, timeout=25)
                logger.info(f"  POST status: {resp.status_code}, Content-Type: {resp.headers.get('content-type')}")
                csv_text = self._extract_csv_text(resp)
                if csv_text:
                    return csv_text
                json_records = self._extract_json_records(resp)
                if json_records:
                    logger.info(f"  JSON payload detected with {len(json_records)} rows")
                    return self._records_to_csv(json_records)
            except Exception as e:
                logger.debug(f"  POST payload failed: {e}")

        logger.warning(f"  No CSV/JSON detected for {url}")
        return None

    @staticmethod
    def _extract_csv_text(resp):
        content_type = resp.headers.get('content-type', '').lower()
        if resp.status_code == 200 and (
            'text/csv' in content_type or
            'application/csv' in content_type or
            'text/plain' in content_type or
            resp.text.startswith('Date,') or
            'Date,' in resp.text[:200]
        ):
            return resp.text
        return None

    @staticmethod
    def _extract_json_records(resp):
        content_type = resp.headers.get('content-type', '').lower()
        text = resp.text.strip()
        if 'application/json' not in content_type and not text.startswith('{') and not text.startswith('['):
            return None
        try:
            data = resp.json()
        except Exception:
            try:
                data = json.loads(text)
            except Exception:
                return None

        # Normalize to list of dicts
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            for key in ['items', 'data', 'content', 'results', 'rows', 'navs']:
                if key in data and isinstance(data[key], list):
                    return [row for row in data[key] if isinstance(row, dict)]
            # Maybe dict keyed by id -> flatten
            if all(isinstance(v, dict) for v in data.values()):
                return [v for v in data.values()]
        return None

    @staticmethod
    def _records_to_csv(records, fieldnames=None):
        if not records:
            return None
        if not fieldnames:
            fieldnames = sorted({key for record in records for key in record.keys()})
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow({k: LightweightVestrScraper._stringify(record.get(k)) for k in fieldnames})
        return buffer.getvalue()

    @staticmethod
    def _stringify(value):
        if value is None:
            return ''
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

def download_csv_lightweight():
    """Main function to download CSV using lightweight scraper"""
    download_dir = os.path.join(os.getcwd(), "uploads")
    os.makedirs(download_dir, exist_ok=True)
    
    try:
        scraper = LightweightVestrScraper()
        
        # Login
        scraper.login()
        
        # Download CSV
        result = scraper.download_csv(download_dir)
        return result
        
    except Exception as e:
        logger.exception("Lightweight scraper failed")
        return {'success': False, 'error': str(e)}
