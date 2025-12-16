# ✅ Enhanced Integrated Sync - Complete Documentation

## 🎯 What's New

The enhanced version includes:
- ✅ **Robust Dropbox uploads** with retry logic, rate limit handling, and exponential backoff
- ✅ **Automatic local file cleanup** with `--delete-after-upload` flag
- ✅ **Vestr fee data synchronization** from GraphQL API with authentication
- ✅ **Database aggregation** with monthly/daily/product fee summaries
- ✅ **Fee snapshot population** for dashboard visualization
- ✅ **Comprehensive file logging** with automatic rotation (10MB max, 5 backups)
- ✅ **Failed upload tracking** with account number reporting
- ✅ **All credentials embedded** (Credinvest, Dropbox, Vestr, PostgreSQL)

---

## 📦 What's Included

### Built Executables
- `dist\integrated_sync.exe` (21 MB) - Original version (Credinvest + snapshots only)
- `dist\integrated_sync_enhanced.exe` (23 MB) - **RECOMMENDED** - Full featured version

### Embedded Credentials
✅ **Credinvest SFTP**
- Username: `esaisfg.5b`
- Password: (embedded)
- SSH private key: (embedded)

✅ **Dropbox OAuth**
- App Key + Secret: (embedded)
- Refresh token: from `dropbox_credentials.json`

✅ **Vestr Authentication**
- Username: `crudi`
- Password: (embedded)
- OTP Secret: (embedded for TOTP generation)

✅ **PostgreSQL Database**
- Default URL: Render PostgreSQL (embedded)
- Override with `DATABASE_URL` environment variable

✅ **Google Sheets** (optional, for future use)
- Service account credentials: from `gsheet_credentials.json`

---

## 🚀 Quick Start

### Run Everything (Recommended for Daily Sync)
```powershell
.\dist\integrated_sync_enhanced.exe
```

This will:
1. Download latest Credinvest INTE files from SFTP
2. Upload to Dropbox `/cred/` folder (with retry logic)
3. Fetch latest Vestr fee data via GraphQL
4. Aggregate and store in PostgreSQL database
5. Rebuild fee snapshots for dashboard queries
6. Create detailed log file in `logs/` folder

### Customization Examples

**Skip Credinvest (only update fees):**
```powershell
.\dist\integrated_sync_enhanced.exe --skip-credinvest
```

**Skip Vestr fees (only Credinvest):**
```powershell
.\dist\integrated_sync_enhanced.exe --skip-vestr-fees
```

**Delete local files after upload:**
```powershell
.\dist\integrated_sync_enhanced.exe --delete-after-upload
```

**Custom download directory:**
```powershell
.\dist\integrated_sync_enhanced.exe --download-dir "C:\my\custom\path"
```

**Verbose logging:**
```powershell
.\dist\integrated_sync_enhanced.exe --verbose
```

**Custom log file:**
```powershell
.\dist\integrated_sync_enhanced.exe --log-file "C:\logs\sync.log"
```

---

## 📊 What Each Task Does

### Task 1: Credinvest SFTP Sync
**Purpose:** Download latest client files and upload to Dropbox

**Process:**
1. Connect to `ext01.credinvest.ch` via SFTP
2. List all files matching pattern: `<account>-<timestamp>-INTE<100/400>F.xlsx`
3. Apply selection rules:
   - Keep latest file per account per type (100F / 400F)
   - If 100F and 400F are >1 day apart, keep only newer one
   - Skip files more than 3 days older than the newest file globally
4. Download selected files to `download/` folder
5. Clear existing files in Dropbox `/cred/` folder
6. Upload files to Dropbox (with retry logic):
   - First pass: try all files once (0.5s delay between uploads)
   - Retry failed uploads 3 times (1s delay between retries)
   - Honor Dropbox `retry_after` on 429 rate limits
   - Report failed account numbers at the end
7. Optionally delete local files after successful upload

**Credentials used:**
- Credinvest SFTP: embedded username/password/SSH key
- Dropbox: refresh token from `dropbox_credentials.json`

### Task 2: Vestr Fee Data Sync
**Purpose:** Fetch latest fee deductions from Vestr and store in PostgreSQL

**Process:**
1. Ensure database tables exist (`vestr_fee_records`, summaries, etc.)
2. Login to Vestr using Keycloak authentication:
   - Submit username + password
   - Generate TOTP code using embedded OTP secret
   - Handle auto-forms and redirects
3. Query GraphQL API for fee deductions:
   - Fetch incremental data (last 30 days by default)
   - Parse booking dates, amounts, fee types, products
4. Upsert records into `vestr_fee_records` table (PostgreSQL)
5. Aggregate data into summary tables:
   - `vestr_fee_monthly_summaries` - per month/product/fee_type
   - `vestr_fee_daily_summaries` - per day/product/fee_type
   - `vestr_fee_product_totals` - lifetime totals per product

**Credentials used:**
- Vestr: embedded username/password/OTP secret
- PostgreSQL: `DATABASE_URL` env var or embedded default

**Database tables created:**
- `vestr_fee_records` - raw fee deductions
- `vestr_fee_monthly_summaries` - monthly aggregates
- `vestr_fee_daily_summaries` - daily aggregates
- `vestr_fee_product_totals` - product lifetime totals
- `fee_sync_status` - sync tracking metadata

### Task 3: Fee Snapshot Population
**Purpose:** Build latest fee snapshot per product for dashboard queries

**Process:**
1. Query all products from `vestr_fee_records`
2. For each product, find:
   - Latest management fee (date + amount)
   - Latest performance fee (date + amount)
   - Latest custody fee (date + amount)
   - Most recent fee overall (any type)
   - Current outstanding quantity (units)
3. Rebuild `fee_latest_snapshot` table with one row per product
4. Show summary statistics (products processed, sample data)

**Credentials used:**
- PostgreSQL: `DATABASE_URL` env var or embedded default

**Database table:**
- `fee_latest_snapshot` - one row per product with latest fee info

---

## 📝 Logging

### Console Output
- Info level by default
- Use `--verbose` or `-v` for debug level
- Shows progress, summaries, and errors
- Color emoji indicators (✅ ❌ ⚠️ 🔄)

### Log Files
**Location:** `logs/integrated_sync_YYYYMMDD.log`

**Features:**
- Automatic rotation (10 MB max file size)
- Keeps 5 backup files
- Debug level (captures everything)
- Timestamped entries with logger names

**Log file includes:**
- All operations (SFTP, Dropbox, Vestr, Database)
- Retry attempts and backoff delays
- Error stack traces
- Authentication flows (without exposing secrets)
- Database query summaries

**Override log location:**
```powershell
.\dist\integrated_sync_enhanced.exe --log-file "C:\custom\path\my_sync.log"
```

---

## 🔧 Troubleshooting

### Dropbox 429 Rate Limit Errors
**Symptom:** `ERROR ... 429 {"error_summary": "too_many_write_operations"`

**Solution:** The executable now handles this automatically:
- Honors `retry_after` from Dropbox response
- Retries failed uploads 3 times with delays
- Reports failed account numbers at the end

**Manual retry:**
```powershell
# Run again - it will try to upload remaining files in download/ folder
.\dist\integrated_sync_enhanced.exe
```

### SFTP Connection Failures
**Symptom:** `SFTP connection failed: [Errno...`

**Check:**
1. Network connectivity to `ext01.credinvest.ch:22`
2. Firewall rules allowing outbound SSH
3. Credentials valid (embedded in EXE)

**Override credentials:**
```powershell
$env:CREDINVEST_SFTP_HOST = "your.host.com"
$env:CREDINVEST_SFTP_USERNAME = "youruser"
$env:CREDINVEST_SFTP_PASSWORD = "yourpass"
.\dist\integrated_sync_enhanced.exe
```

### Vestr Login Failures
**Symptom:** `OTP verification failed` or `Login form not found`

**Possible causes:**
- Keycloak server down/slow
- OTP time drift (system clock incorrect)
- Vestr password changed

**Check:**
1. System clock is accurate (OTP is time-based)
2. Can access https://aisfg.delta.vestr.com manually
3. Check logs for detailed error messages

### Database Connection Errors
**Symptom:** `Database connection failed` or `SSL timeout`

**Check:**
1. Network connectivity to Render PostgreSQL
2. Database URL is correct
3. SSL certificates valid

**Override database URL:**
```powershell
$env:DATABASE_URL = "postgresql://user:pass@host:port/dbname"
.\dist\integrated_sync_enhanced.exe
```

### Failed Account Numbers Reported
**Symptom:** `Failed account numbers: Account 12345 - INTE100F`

**Meaning:** Those specific files failed to upload after 3 retries

**Action:**
1. Check logs for detailed error (rate limit vs network vs credentials)
2. Files remain in `download/` folder
3. Re-run executable to retry (or use `--skip-credinvest` to skip re-download)

### Missing Local Files After Run
**Symptom:** `download/` folder is empty but uploads succeeded

**Cause:** Used `--delete-after-upload` flag

**Normal behavior:** Local files are deleted after successful upload to save disk space

**Keep files:** Don't use `--delete-after-upload` flag

---

## 🕒 Scheduling

### Windows Task Scheduler Setup

1. **Open Task Scheduler:**
   ```
   Win + R → taskschd.msc
   ```

2. **Create Basic Task:**
   - Name: `Daily Integrated Sync`
   - Description: `Sync Credinvest files and Vestr fees daily`

3. **Trigger:**
   - Daily
   - Start time: `17:10` (5:10 PM - after market close)
   - Recur every: `1 day`

4. **Action:**
   - Action: `Start a program`
   - Program: `C:\Users\...\Desktop\AlwaysOnPC\dist\integrated_sync_enhanced.exe`
   - Arguments: `--delete-after-upload` (optional)
   - Start in: `C:\Users\...\Desktop\AlwaysOnPC`

5. **Conditions:**
   - ☑️ Start only if on AC power (uncheck for laptops)
   - ☑️ Wake the computer to run this task

6. **Settings:**
   - ☑️ Allow task to be run on demand
   - ☑️ Run task as soon as possible after a scheduled start is missed
   - If task is running, then: `Do not start a new instance`

### Verification
```powershell
# Test the task manually first
.\dist\integrated_sync_enhanced.exe --verbose

# Check logs after scheduled run
Get-Content "logs\integrated_sync_*.log" -Tail 50
```

---

## 📂 Files & Structure

```
AlwaysOnPC/
├── dist/
│   ├── integrated_sync.exe (21 MB) - Original version
│   └── integrated_sync_enhanced.exe (23 MB) ⭐ RECOMMENDED
├── logs/
│   └── integrated_sync_20251215.log (auto-created, rotated)
├── download/ (created automatically)
│   └── <account>-<timestamp>-INTE<type>.xlsx (downloaded files)
├── dropbox_credentials.json ✅ (embedded in EXE)
├── gsheet_credentials.json ✅ (embedded in EXE)
├── credinvest_sync.py (source)
├── vestr_lightweight.py (source)
├── vestr_fees_lightweight.py (source)
├── database_models.py (source)
├── populate_fee_snapshots.py (source)
├── integrated_sync_enhanced.py (source)
└── integrated_sync_enhanced.spec (PyInstaller config)
```

---

## 🎯 Summary of Features

### Dropbox Upload Improvements
✅ Retry failed uploads 3 times automatically
✅ Honor Dropbox `retry_after` on 429 rate limits
✅ Exponential backoff (1s, 2s, 4s...)
✅ Per-upload delays to avoid burst rate limits
✅ Report failed account numbers at end
✅ Optional auto-delete local files after upload

### Vestr Fee Synchronization
✅ Automated Keycloak login with OTP
✅ GraphQL API query for fee deductions
✅ Incremental sync (last 30 days by default)
✅ Robust retry logic for auth failures
✅ Store raw records in PostgreSQL
✅ Auto-aggregate into monthly/daily/product summaries
✅ Rebuild snapshots for dashboard queries

### Logging & Monitoring
✅ Rotating log files (10 MB max, 5 backups)
✅ Detailed error messages with stack traces
✅ Failed upload tracking by account number
✅ Sync session summaries
✅ Verbose mode for debugging

### Credentials & Security
✅ All credentials embedded in EXE
✅ No external config files required (except JSON tokens)
✅ Environment variable overrides supported
✅ Secrets never logged (passwords/OTP/tokens redacted)

---

## 🆘 Support

### Check Logs First
```powershell
# View recent logs
Get-Content "logs\integrated_sync_*.log" -Tail 100

# Search for errors
Select-String -Path "logs\*.log" -Pattern "ERROR|CRITICAL"
```

### Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| 429 rate limit | Already handled automatically; just wait and retry |
| SFTP timeout | Check network/firewall; credentials embedded |
| OTP failure | Check system clock; OTP is time-based |
| Database SSL | Check Render DB status; embedded URL should work |
| Missing snapshots | Run with `--skip-credinvest --skip-vestr-fees` to only rebuild snapshots |

### Force Full Fee Sync
If you need to re-fetch all historical data (not just last 30 days):
```python
# Edit vestr_fees_lightweight.py temporarily:
result = sync_fees_dataset(force_full=True)

# Or create a custom script:
from vestr_fees_lightweight import sync_fees_dataset
sync_fees_dataset(force_full=True)
```

---

## ✅ Final Checklist

Before deploying to always-on PC:

- [ ] Test executable locally: `.\dist\integrated_sync_enhanced.exe --verbose`
- [ ] Verify all 3 tasks complete successfully
- [ ] Check logs created in `logs/` folder
- [ ] Verify Dropbox uploads (check `/cred/` folder)
- [ ] Verify database records (query PostgreSQL)
- [ ] Copy EXE to target PC
- [ ] Set up Windows Task Scheduler
- [ ] Run task manually once to verify
- [ ] Monitor first few scheduled runs

---

## 📞 Quick Reference

```powershell
# Full sync (recommended)
.\dist\integrated_sync_enhanced.exe

# Credinvest only
.\dist\integrated_sync_enhanced.exe --skip-vestr-fees --skip-fee-snapshots

# Vestr fees only
.\dist\integrated_sync_enhanced.exe --skip-credinvest

# With auto-cleanup
.\dist\integrated_sync_enhanced.exe --delete-after-upload

# Verbose mode
.\dist\integrated_sync_enhanced.exe -v

# Help
.\dist\integrated_sync_enhanced.exe --help
```

**Log location:** `logs/integrated_sync_YYYYMMDD.log`

**Executable size:** 23 MB (all-in-one, no dependencies needed)

**Runs on:** Any Windows machine (no Python required)

🎉 **You're all set!**
