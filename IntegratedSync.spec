# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['integrated_sync_enhanced.py'],
    pathex=[],
    binaries=[],
    datas=[('dropbox_credentials.json', '.'), ('gsheet_credentials.json', '.')],
    hiddenimports=['sqlalchemy.dialects.postgresql', 'sqlalchemy.dialects.postgresql.psycopg2', 'psycopg2', 'pyotp', 'paramiko', 'dropbox', 'gspread', 'vestr_fees_lightweight', 'populate_fee_snapshots', 'database_models', 'credinvest_sync'],
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
    name='IntegratedSync',
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
