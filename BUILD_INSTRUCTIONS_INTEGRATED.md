# Integrated Sync - Build Instructions

## Overview
This project combines:
1. **Credinvest SFTP sync** - Downloads INTE files and uploads to Dropbox
2. **Vestr fee snapshot population** - Updates PostgreSQL database with latest fee data

The executable runs both tasks in sequence.

---

## Prerequisites

### 1. Install Python Dependencies
```powershell
cd "C:\Users\MohammadmahdiRahbari\Desktop\AlwaysOnPC"
pip install -r requirements.txt
```

### 2. Ensure Credential Files Exist
Make sure these files are in the AlwaysOnPC folder:
- `dropbox_credentials.json` ✅ (already present)
- `gsheet_credentials.json` ✅ (copied from aisrender)

### 3. Environment Variables (Optional)
Set these if needed (the script has defaults):
- `DATABASE_URL` - PostgreSQL connection string
- `CREDINVEST_SFTP_HOST` - SFTP server hostname
- `CREDINVEST_SFTP_USERNAME` - SFTP username
- `CREDINVEST_SFTP_PASSWORD` - SFTP password

---

## 🚀 Quick Build (Single Command)

### Option 1: Using spec file (Recommended)
```powershell
cd "C:\Users\MohammadmahdiRahbari\Desktop\AlwaysOnPC"
pyinstaller integrated_sync.spec
```

### Option 2: One-line command
```powershell
cd "C:\Users\MohammadmahdiRahbari\Desktop\AlwaysOnPC"
pyinstaller --onefile --name integrated_sync --add-data "dropbox_credentials.json;." --add-data "gsheet_credentials.json;." --hidden-import paramiko --hidden-import paramiko.rsakey --hidden-import paramiko.ed25519key --hidden-import requests --hidden-import cryptography --hidden-import sqlalchemy --hidden-import sqlalchemy.dialects.postgresql --hidden-import psycopg2 --hidden-import credinvest_sync --hidden-import populate_fee_snapshots --hidden-import database_models --console integrated_sync.py
```

---

## 📦 Build Output

After running PyInstaller:
- **EXE file**: `dist\integrated_sync.exe`
- **Build artifacts**: `build\` folder (can be deleted after successful build)

The EXE is completely standalone and includes:
- All Python dependencies
- Embedded credential files (`dropbox_credentials.json`, `gsheet_credentials.json`)
- Database models and sync logic

---

## ▶️ Running the Executable

### Run both tasks (default):
```powershell
.\dist\integrated_sync.exe
```

### Custom download directory:
```powershell
.\dist\integrated_sync.exe --download-dir "C:\temp\downloads"
```

### Run only Credinvest sync (skip fees):
```powershell
.\dist\integrated_sync.exe --skip-fees
```

### Run only fee snapshot (skip Credinvest):
```powershell
.\dist\integrated_sync.exe --skip-credinvest
```

### Help:
```powershell
.\dist\integrated_sync.exe --help
```

---

## 📋 What the Executable Does

### Task 1: Credinvest SFTP Sync
1. Connects to Credinvest SFTP server
2. Lists and filters INTE100F/INTE400F files
3. Applies selection rules (latest per client/type)
4. Downloads selected files to local `download/` folder
5. Wipes existing files in Dropbox `/cred/` folder
6. Uploads downloaded files to Dropbox

### Task 2: Fee Snapshot Population
1. Connects to PostgreSQL database (Render)
2. Ensures fee tables exist
3. Queries `vestr_fee_records` table
4. Calculates latest management/performance/custody fees per product
5. Rebuilds `fee_latest_snapshot` table
6. Shows summary statistics

---

## 🔧 Troubleshooting

### Build fails with import errors
Make sure all dependencies are installed:
```powershell
pip install -r requirements.txt
```

### EXE crashes on database connection
- Check `DATABASE_URL` environment variable or default in `database_models.py`
- Verify network connectivity to PostgreSQL server
- Test database connection manually first

### SFTP connection fails
- Verify `CREDINVEST_SFTP_*` environment variables
- Check firewall/network settings
- Test SFTP connection with another tool (FileZilla, WinSCP)

### Dropbox upload fails
- Check `dropbox_credentials.json` contains valid refresh_token
- Verify Dropbox app permissions
- Token may need refresh if expired

### Missing credential files in EXE
- Ensure `.json` files exist in AlwaysOnPC folder before building
- Check `integrated_sync.spec` includes files in `datas=` section
- Rebuild with `pyinstaller integrated_sync.spec`

---

## 📝 Files Included in Build

### Source Files:
- `integrated_sync.py` - Main entry point
- `credinvest_sync.py` - Credinvest SFTP logic
- `populate_fee_snapshots.py` - Fee snapshot logic
- `database_models.py` - SQLAlchemy models

### Data Files (Embedded):
- `dropbox_credentials.json` - Dropbox OAuth token
- `gsheet_credentials.json` - Google Sheets credentials

### Dependencies (Bundled):
- paramiko, requests, cryptography - SFTP/HTTP
- sqlalchemy, psycopg2 - Database ORM and PostgreSQL driver

---

## 🎯 Deployment

The EXE can be:
1. **Scheduled** via Windows Task Scheduler
2. **Copied** to any Windows machine (no Python needed)
3. **Run remotely** via RDP or remote command

### Example: Windows Task Scheduler
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (e.g., Daily at 5:00 PM)
4. Action: Start a program
5. Program: `C:\path\to\integrated_sync.exe`
6. Save and test

---

## ✅ Success Indicators

The executable will show:
```
================================================================================
INTEGRATED SYNC - AlwaysOnPC
================================================================================

[1/2] Running Credinvest SFTP sync...
...
✅ Credinvest sync completed

[2/2] Running fee snapshot population...
...
✅ Fee snapshot population completed

================================================================================
✅ INTEGRATED SYNC COMPLETE - ALL TASKS SUCCESSFUL
================================================================================
```

---

## 📚 Additional Resources

- Original Credinvest script: `credinvest_sync.py`
- Database models documentation: See `database_models.py` docstrings
- Render database URL: Check `database_models.py` or set `DATABASE_URL` env var

For issues or questions, check the logs in the console output.
