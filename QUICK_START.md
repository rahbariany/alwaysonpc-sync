# 🎯 QUICK START GUIDE - Integrated Sync Executable

## ✅ Build Complete!

Your executable is ready: `dist\integrated_sync.exe` (21 MB)

---

## 🚀 Quick Commands

### Run everything (recommended):
```powershell
.\dist\integrated_sync.exe
```

### Custom download location:
```powershell
.\dist\integrated_sync.exe --download-dir "C:\my\custom\path"
```

### Run only Credinvest sync:
```powershell
.\dist\integrated_sync.exe --skip-fees
```

### Run only fee snapshots:
```powershell
.\dist\integrated_sync.exe --skip-credinvest
```

---

## 📋 What It Does

1. **Credinvest SFTP Sync**
   - Downloads INTE100F/INTE400F files from Credinvest SFTP
   - Applies selection rules (latest per client)
   - Uploads to Dropbox `/cred/` folder

2. **Fee Snapshot Population**
   - Connects to PostgreSQL database
   - Rebuilds `fee_latest_snapshot` table
   - Updates with latest fee data per product

---

## 🔧 Requirements

### Embedded in EXE:
- ✅ All Python dependencies
- ✅ `dropbox_credentials.json`
- ✅ `gsheet_credentials.json`

### External (needed at runtime):
- ✅ Internet connection (for SFTP/Dropbox/Database)
- ✅ Valid credentials in JSON files
- ✅ Database connection (default: Render PostgreSQL)

---

## 📁 Project Files

### Created/Updated:
- `integrated_sync.py` - Main integrated script
- `database_models.py` - SQLAlchemy models for fees
- `populate_fee_snapshots.py` - Fee snapshot logic
- `integrated_sync.spec` - PyInstaller configuration
- `requirements.txt` - Python dependencies
- `BUILD_INSTRUCTIONS_INTEGRATED.md` - Detailed build guide

### Preserved:
- `credinvest_sync.py` - Original Credinvest logic (imported)
- `dropbox_credentials.json` - Dropbox OAuth token
- `gsheet_credentials.json` - Google Sheets credentials (copied)

---

## 🎯 Deployment Options

### Option 1: Windows Task Scheduler
1. Open Task Scheduler
2. Create Basic Task → Daily at 5:00 PM
3. Action: `C:\path\to\integrated_sync.exe`

### Option 2: Manual Run
Double-click `integrated_sync.exe` or run from PowerShell

### Option 3: Remote Execution
Copy `integrated_sync.exe` to target machine and schedule/run

---

## 🔄 Rebuild Instructions

If you need to rebuild (e.g., after updating credentials):

```powershell
cd "C:\Users\MohammadmahdiRahbari\Desktop\AlwaysOnPC"
pyinstaller integrated_sync.spec
```

The new EXE will be in `dist\` folder.

---

## 📊 Expected Output

```
================================================================================
INTEGRATED SYNC - AlwaysOnPC
================================================================================

[1/2] Running Credinvest SFTP sync...
--------------------------------------------------------------------------------
Using SFTP host=ext01.credinvest.ch user=esaisfg.5b port=22
Found 47 remote files (listing filtered by regex)
Files matching target pattern: 12
Selected 2 files to download
Downloading INTE100F_ESA_20251215_001.txt
Downloading INTE400F_ESA_20251215_001.txt
Wiping Dropbox folder before upload: /cred/
Uploading to Dropbox: /cred/INTE100F_ESA_20251215_001.txt
Uploading to Dropbox: /cred/INTE400F_ESA_20251215_001.txt
✅ Credinvest sync completed

[2/2] Running fee snapshot population...
--------------------------------------------------------------------------------
================================================================================
POPULATE FEE SNAPSHOTS
================================================================================

📊 Current State:
   Raw records: 45823
   Snapshot records (before): 156

🔧 Processing 156 products...

✅ Snapshots Created:
   Inserted: 156
   Total snapshot records: 156

📋 Sample Snapshots (first 5):
   Product ABC Fund               | Units:   12345.67 | Last: 2025-12-14
   Product XYZ Strategy           | Units:    8901.23 | Last: 2025-12-13
   ...

================================================================================
COMPLETE
================================================================================
✅ Fee snapshot population completed

================================================================================
✅ INTEGRATED SYNC COMPLETE - ALL TASKS SUCCESSFUL
================================================================================
```

---

## ⚠️ Troubleshooting

### EXE won't run / crashes immediately
- Check Windows Defender/Antivirus (may block unsigned EXE)
- Run from PowerShell to see error messages

### SFTP connection fails
- Verify network/firewall settings
- Check SFTP credentials in environment variables or defaults

### Database connection fails
- Verify `DATABASE_URL` environment variable
- Check network connectivity to Render PostgreSQL
- Test connection: `psql <DATABASE_URL>`

### Dropbox upload fails
- Refresh token may be expired → regenerate `dropbox_credentials.json`
- Check Dropbox app permissions

---

## 📞 Support

For detailed information:
- See `BUILD_INSTRUCTIONS_INTEGRATED.md`
- Check console output for specific error messages
- Review log files in terminal/console

---

## 🎉 Success!

Your integrated sync executable is ready for deployment!
