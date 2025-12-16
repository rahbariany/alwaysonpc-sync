# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for integrated_sync_enhanced.exe
Includes all dependencies, credential files, and Vestr scraping support
"""

block_cipher = None

a = Analysis(
    ['integrated_sync_enhanced.py'],
    pathex=[],
    binaries=[],
    datas=[
        # Credential files
        ('dropbox_credentials.json', '.'),
        ('gsheet_credentials.json', '.'),
    ],
    hiddenimports=[
        # Core dependencies
        'paramiko',
        'paramiko.rsakey',
        'paramiko.ed25519key',
        'paramiko.ecdsakey',
        'paramiko.dsskey',
        'requests',
        'requests.packages.urllib3',
        'cryptography',
        'cryptography.hazmat.primitives.asymmetric.rsa',
        'cryptography.hazmat.primitives.asymmetric.ed25519',
        'cryptography.hazmat.backends.openssl',
        # SQLAlchemy dependencies
        'sqlalchemy',
        'sqlalchemy.ext.declarative',
        'sqlalchemy.orm',
        'sqlalchemy.sql',
        'sqlalchemy.sql.default_comparator',
        'sqlalchemy.engine',
        'sqlalchemy.pool',
        'sqlalchemy.dialects.postgresql',
        'sqlalchemy.dialects.postgresql.psycopg2',
        # PostgreSQL driver
        'psycopg2',
        'psycopg2.extensions',
        'psycopg2.extras',
        # Vestr scraping
        'bs4',
        'beautifulsoup4',
        'pyotp',
        'lxml',
        'lxml.etree',
        'lxml._elementpath',
        # Our modules
        'credinvest_sync',
        'populate_fee_snapshots',
        'database_models',
        'vestr_lightweight',
        'vestr_fees_lightweight',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='integrated_sync_enhanced',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # Keep console window for logging output
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None
)
