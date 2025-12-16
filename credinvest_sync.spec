# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['credinvest_sync.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('dropbox_credentials.json', '.'),  # Include credentials file in root of EXE
    ],
    hiddenimports=[
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
    name='credinvest_sync',
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
    icon=None,
)
