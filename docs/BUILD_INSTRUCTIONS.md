# Credinvest Sync - Build Instructions

## Prerequisites
```powershell
# Install PyInstaller if not already installed
pip install pyinstaller

# Ensure all dependencies are installed
pip install paramiko requests cryptography
```

## ⚡ Quick Build Command (Copy & Paste)

```powershell
cd "C:\Users\MohammadmahdiRahbari\Desktop\AlwaysOnPC"
pyinstaller --onefile --name credinvest_sync --add-data "dropbox_credentials.json;." --hidden-import paramiko --hidden-import paramiko.rsakey --hidden-import paramiko.ed25519key --hidden-import requests --hidden-import cryptography --hidden-import cryptography.hazmat.primitives.asymmetric.rsa --hidden-import cryptography.hazmat.primitives.asymmetric.ed25519 --hidden-import cryptography.hazmat.backends.openssl --console credinvest_sync.py
```

## Build Options

### Option 1: Simple one-file EXE (Recommended - see above)
This creates a single .exe file with everything embedded including the Dropbox refresh token.

### Option 2: Using spec file (More control)
```powershell
cd "C:\Users\MohammadmahdiRahbari\Desktop\AlwaysOnPC"
pyinstaller credinvest_sync.spec
```

## Output Location
- EXE file: `dist\credinvest_sync.exe`
- Build artifacts: `build\` folder (can be deleted after build)

## Important Notes
1. The `dropbox_credentials.json` file will be embedded in the EXE
2. The EXE is self-contained and can run on any Windows machine without Python installed
3. You can distribute just the EXE file - no other files needed
4. The EXE will look for `dropbox_credentials.json` next to itself first, then use embedded version

## Testing the EXE
```powershell
# Test the built EXE
.\dist\credinvest_sync.exe --download-dir "C:\temp\downloads"

# Or just run with defaults
.\dist\credinvest_sync.exe
```

## Troubleshooting
If you get import errors:
```powershell
# Install missing modules
pip install --upgrade paramiko cryptography pywin32
```

If credentials don't work:
- Ensure `dropbox_credentials.json` is in the same folder as the script when building
- The refresh token in the file should be valid
- Test the Python script first before building EXE

## File Size
Expected EXE size: ~25-35 MB (includes Python runtime + all dependencies)
