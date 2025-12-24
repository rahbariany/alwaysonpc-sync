# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['integrated_sync_enhanced.py'],
    pathex=['C:\\Users\\MohammadmahdiRahbari\\Desktop\\amc automate\\ais-amc-automate'],
    binaries=[],
    datas=[('gsheet_credentials.json', '.'), ('dropbox_credentials.json', '.'), ('C:\\Users\\MohammadmahdiRahbari\\Desktop\\amc automate\\ais-amc-automate', 'ais-amc-automate')],
    hiddenimports=['credinvest_sync', 'vestr_fees_lightweight', 'populate_fee_snapshots', 'database_models', 'pywintypes', 'pythoncom', 'win32com', 'win32timezone', 'pyotp', 'bs4', 'requests', 'gspread', 'google.oauth2.service_account', 'pandas._libs.tslibs.timedeltas', 'pandas._libs.tslibs.nattype', 'openpyxl', 'xlsxwriter'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='integrated_sync_enhanced',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
