#!/usr/bin/env python3
"""
credinvest_sync.py

Single-file script to download Credinvest INTE100F/INTE400F files from Credinvest SFTP
and upload them to the Dropbox app folder `AISAMC` under `/cred/`.

Usage:
  python credinvest_sync.py [--download-dir PATH]

Dependencies:
  pip install paramiko requests

This script uses the Credinvest credentials embedded in the aisrender project (copied defaults),
but will respect environment variable overrides (CREDINVEST_SFTP_HOST, CREDINVEST_SFTP_PORT,
CREDINVEST_SFTP_USERNAME, CREDINVEST_SFTP_PASSWORD, CREDINVEST_SFTP_PRIVATE_KEY).
"""
import io
import os
import sys
import re
import time
import json
import logging
import argparse
import datetime
from collections import defaultdict

try:
    import paramiko
except Exception as e:
    print("Missing dependency: paramiko. Install with `pip install paramiko`")
    raise

try:
    import requests
except Exception:
    print("Missing dependency: requests. Install with `pip install requests`")
    raise

LOG = logging.getLogger("credinvest_sync")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Default Credinvest SFTP config (copied from aisrender project)
DEFAULT_CONFIG = {
    'host': os.getenv('CREDINVEST_SFTP_HOST', os.getenv('CREDINVEST_TUNNEL_HOST', 'ext01.credinvest.ch')),
    'port': int(os.getenv('CREDINVEST_SFTP_PORT', os.getenv('CREDINVEST_TUNNEL_PORT', 22))),
    'username': os.getenv('CREDINVEST_SFTP_USERNAME', os.getenv('CREDINVEST_USERNAME', 'esaisfg.5b')),
    # Embedded fallback password (from project). Can be overridden with env var.
    'password': os.getenv('CREDINVEST_SFTP_PASSWORD', os.getenv('CREDINVEST_PASSWORD', 'byx9wb3.y1')),
    # Private key fallback — if you prefer key auth set CREDINVEST_SFTP_PRIVATE_KEY env var
    'private_key': os.getenv('CREDINVEST_SFTP_PRIVATE_KEY', None) or '''-----BEGIN OPENSSH PRIVATE KEY-----
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
BQYHCAkK
-----END OPENSSH PRIVATE KEY-----'''
}

# If no PRIVATE_KEY env var provided, we keep None — the script will try password auth

# Dropbox configuration (OAuth2 with refresh token)
# The script reads the refresh token from dropbox_credentials.json in the same directory
# This refresh token never expires and automatically renews access tokens
DROPBOX_APP_KEY = 'bnavdhduj8ux50k'
DROPBOX_APP_SECRET = '0hp399rberhd2bz'
DROPBOX_CREDENTIALS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dropbox_credentials.json')
DROPBOX_TARGET_FOLDER = os.getenv('DROPBOX_TARGET_FOLDER', '/cred')  # relative to app folder

def get_dropbox_token():
    """Load refresh token and get access token using OAuth2."""
    if os.path.exists(DROPBOX_CREDENTIALS_FILE):
        try:
            with open(DROPBOX_CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
                creds = json.load(f)
            refresh_token = creds.get('refresh_token')
            if refresh_token and refresh_token != "PLACEHOLDER_WILL_BE_GENERATED_ON_FIRST_RUN":
                # Get access token using refresh token
                data = {
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token,
                    'client_id': DROPBOX_APP_KEY,
                    'client_secret': DROPBOX_APP_SECRET
                }
                r = requests.post('https://api.dropbox.com/oauth2/token', data=data)
                if r.status_code == 200:
                    return r.json()['access_token']
                else:
                    LOG.error(f"Failed to refresh Dropbox token: {r.status_code} {r.text}")
        except Exception as e:
            LOG.error(f"Error loading Dropbox credentials: {e}")
    
    LOG.error("No valid Dropbox refresh token found. Please run the auth setup first.")
    LOG.error(f"Expected file: {DROPBOX_CREDENTIALS_FILE}")
    return None

# Filename regex and helper functions (copied from CombinedAutomation logic)
FNAME_RE = re.compile(r"(?P<cid>\d+)-(?P<ts>\d{14})-(?P<type>INTE(?:100|400)F)\.xlsx$")

def parse_timestamp_from_filename(fname: str):
    m = FNAME_RE.match(fname)
    if not m:
        return None
    ts = m.group('ts')
    try:
        return datetime.datetime.strptime(ts, "%Y%m%d%H%M%S")
    except Exception:
        return None

def group_files_by_client(filenames):
    files_by_client = defaultdict(lambda: {"100F": [], "400F": []})
    for fname in filenames:
        m = FNAME_RE.match(fname)
        if not m:
            continue
        dt = parse_timestamp_from_filename(fname)
        if not dt:
            continue
        ftype = m.group('type')[-4:]
        cid = m.group('cid')
        files_by_client[cid][ftype].append((fname, dt.date()))
    return files_by_client

def pick_latest_per_type(grouped):
    latest = {}
    for cid, types in grouped.items():
        latest[cid] = {}
        for ftype, entries in types.items():
            if entries:
                latest[cid][ftype] = max(entries, key=lambda x: x[1])
    return latest

def apply_rule1(latest):
    to_download = []
    for cid, files in latest.items():
        d100 = files.get('100F')
        d400 = files.get('400F')
        if d100 and d400:
            diff_days = abs((d100[1] - d400[1]).days)
            if diff_days > 1:
                newer = d100 if d100[1] > d400[1] else d400
                to_download.append(newer)
                LOG.info(f"Client {cid}: Files >1 day apart, keeping newer: {newer[0]}")
                continue
        if d100:
            to_download.append(d100)
        if d400:
            to_download.append(d400)
    return to_download

def apply_rule2(candidates):
    global_max = {}
    for fname, dt in candidates:
        ftype = '100F' if '100F' in fname else '400F'
        global_max[ftype] = max(global_max.get(ftype, dt), dt)
    final_files = []
    for fname, dt in candidates:
        ftype = '100F' if '100F' in fname else '400F'
        if abs((global_max[ftype] - dt).days) <= 3:
            final_files.append(fname)
        else:
            LOG.info(f"Skipping {fname}: {ftype} is {(global_max[ftype] - dt).days} days behind global max")
    return final_files

def load_private_key_from_text(key_text: str):
    if not key_text:
        return None
    text = key_text.strip()
    if 'BEGIN' not in text:
        # try base64 decode
        try:
            import base64
            text = base64.b64decode(text).decode('utf-8')
        except Exception:
            pass
    text = text.replace('\\n', '\n')
    key_file = io.StringIO(text)
    try:
        return paramiko.RSAKey.from_private_key(key_file)
    except Exception:
        key_file.seek(0)
        try:
            return paramiko.Ed25519Key.from_private_key(key_file)
        except Exception:
            return None

def connect_sftp(config):
    LOG.info(f"Connecting to SFTP {config['host']}:{config['port']} as {config.get('username')}")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Prepare key
    pkey = None
    pk_text = config.get('private_key') or ''
    if pk_text:
        pkey = load_private_key_from_text(pk_text)
        if pkey:
            LOG.info("Loaded private key for authentication")

    connect_kwargs = dict(
        hostname=config['host'],
        port=int(config.get('port', 22)),
        username=config.get('username'),
        allow_agent=False,
        look_for_keys=False,
        banner_timeout=60,
        auth_timeout=30,
        timeout=60
    )
    if pkey:
        connect_kwargs['pkey'] = pkey
    if config.get('password'):
        connect_kwargs['password'] = config.get('password')

    if not connect_kwargs.get('pkey') and not connect_kwargs.get('password'):
        raise RuntimeError('No authentication method available (provide password or private key)')

    client.connect(**connect_kwargs)
    try:
        transport = client.get_transport()
        if transport:
            transport.set_keepalive(30)
    except Exception:
        pass
    sftp = client.open_sftp()
    LOG.info('SFTP connected')
    return sftp, client

def download_remote_file(sftp, remote_path, local_path, attempts=3):
    for i in range(1, attempts+1):
        try:
            sftp.get(remote_path, local_path)
            LOG.info(f"Downloaded: {remote_path} -> {local_path}")
            return True
        except Exception as e:
            LOG.warning(f"Attempt {i}/{attempts} failed for {remote_path}: {e}")
            if i < attempts:
                time.sleep(min(5*i, 10))
    LOG.error(f"Failed to download {remote_path} after {attempts} attempts")
    return False

def upload_to_dropbox(local_path, dropbox_path, token=None, attempts=3, skip_on_failure=False):
    """Upload a file to Dropbox with retry logic and exponential backoff.
    
    Args:
        local_path: Local file path to upload
        dropbox_path: Destination path in Dropbox
        token: Dropbox access token (will fetch if None)
        attempts: Number of retry attempts
        skip_on_failure: If True, return False on failure; if False, raise exception
    
    Returns:
        True on success, False on failure (if skip_on_failure=True)
    """
    if token is None:
        token = get_dropbox_token()
    if not token:
        if skip_on_failure:
            return False
        raise RuntimeError('Dropbox access token not available')
    
    url = 'https://content.dropboxapi.com/2/files/upload'
    headers = {
        'Authorization': f'Bearer {token}',
        'Dropbox-API-Arg': json.dumps({
            'path': dropbox_path,
            'mode': 'add',
            'autorename': True,
            'mute': False
        }),
        'Content-Type': 'application/octet-stream'
    }
    
    backoff = 1.0
    for i in range(1, attempts + 1):
        try:
            with open(local_path, 'rb') as f:
                data = f.read()
            r = requests.post(url, headers=headers, data=data, timeout=60)
            
            if r.status_code == 200:
                LOG.info(f"✅ Uploaded {os.path.basename(local_path)} -> Dropbox:{dropbox_path}")
                return True
            elif r.status_code == 429:
                # Dropbox rate limit - honor retry_after if present
                try:
                    info = r.json()
                    retry_after = info.get('error', {}).get('retry_after')
                except Exception:
                    retry_after = None
                wait = float(retry_after) if retry_after else backoff
                if i < attempts:
                    LOG.warning(f"⏳ Dropbox rate limit (429) for {os.path.basename(local_path)}. Attempt {i}/{attempts}. Waiting {wait:.1f}s...")
                    time.sleep(wait)
                else:
                    LOG.error(f"❌ Dropbox rate limit exceeded for {os.path.basename(local_path)} after {attempts} attempts")
            else:
                LOG.error(f"❌ Dropbox upload failed for {os.path.basename(local_path)}: {r.status_code} {r.text[:200]}")
                if i < attempts:
                    time.sleep(backoff)
        except Exception as e:
            if i < attempts:
                LOG.warning(f"⚠️  Upload attempt {i}/{attempts} failed for {os.path.basename(local_path)}: {e}")
                time.sleep(backoff)
            else:
                LOG.error(f"❌ Upload exception for {os.path.basename(local_path)}: {e}")
        
        backoff = min(backoff * 2, 60)
    
    return False

def delete_all_in_dropbox_folder(folder, token=None):
    """Delete all entries in the given Dropbox folder (app folder path).
    Uses list_folder + delete_batch. Returns True on success.
    """
    if token is None:
        token = get_dropbox_token()
    if not token:
        LOG.error('No Dropbox token available for deletion')
        return False
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    list_url = 'https://api.dropboxapi.com/2/files/list_folder'
    list_continue = 'https://api.dropboxapi.com/2/files/list_folder/continue'
    delete_batch_url = 'https://api.dropboxapi.com/2/files/delete_batch'

    # Normalize folder path
    path = folder if folder.startswith('/') else '/' + folder
    data = {'path': path, 'recursive': False}
    try:
        r = requests.post(list_url, headers=headers, json=data)
        if r.status_code == 409:
            # Folder not found — nothing to delete
            LOG.info(f"Dropbox folder {path} not found; nothing to delete")
            return True
        r.raise_for_status()
        resp = r.json()
        entries = resp.get('entries', [])
        # Handle pagination
        while resp.get('has_more'):
            r = requests.post(list_continue, headers=headers, json={'cursor': resp.get('cursor')})
            r.raise_for_status()
            resp = r.json()
            entries.extend(resp.get('entries', []))

        if not entries:
            LOG.info(f"No files found in Dropbox folder {path} to delete")
            return True

        # Build delete entries for batch
        delete_entries = []
        for e in entries:
            # Use path_display or path_lower; ensure leading '/'
            p = e.get('path_lower') or e.get('path_display')
            if p:
                delete_entries.append({'path': p})

        if not delete_entries:
            LOG.info('No deletable entries found')
            return True

        payload = {'entries': delete_entries}
        r = requests.post(delete_batch_url, headers=headers, json=payload)
        if r.status_code in (200, 202):
            LOG.info(f"Requested deletion of {len(delete_entries)} entries in Dropbox folder {path}")
            return True
        else:
            LOG.error(f"Dropbox delete_batch failed: {r.status_code} {r.text}")
            return False

    except Exception as e:
        LOG.error(f"Error deleting Dropbox folder {path}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--download-dir', '-d', default=os.path.join(os.getcwd(), 'download'), help='Local folder to save downloaded files')
    parser.add_argument('--remote-folder', '-r', default='.', help='Remote folder on SFTP server to list files from')
    parser.add_argument('--delete-after-upload', action='store_true', help='Delete local files after successful upload to Dropbox')
    args = parser.parse_args()

    download_dir = os.path.abspath(args.download_dir)
    os.makedirs(download_dir, exist_ok=True)

    # Build config from DEFAULT_CONFIG and environment overrides
    config = DEFAULT_CONFIG.copy()
    # Support older env var names
    config['host'] = os.getenv('CREDINVEST_SFTP_HOST', config['host'])
    config['port'] = int(os.getenv('CREDINVEST_SFTP_PORT', config['port']))
    config['username'] = os.getenv('CREDINVEST_SFTP_USERNAME', config['username'])
    config['password'] = os.getenv('CREDINVEST_SFTP_PASSWORD', config.get('password'))
    pk_env = os.getenv('CREDINVEST_SFTP_PRIVATE_KEY') or os.getenv('CREDINVEST_PRIVATE_KEY')
    if pk_env:
        config['private_key'] = pk_env

    LOG.info(f"Using SFTP host={config['host']} user={config['username']} port={config['port']}")

    # Connect and list files
    sftp = None
    client = None
    try:
        sftp, client = connect_sftp(config)
    except Exception as e:
        LOG.error(f"SFTP connection failed: {e}")
        sys.exit(1)

    try:
        files = sftp.listdir(args.remote_folder)
    except Exception:
        try:
            files = sftp.listdir('.')
        except Exception as e:
            LOG.error(f"Failed to list remote directory: {e}")
            files = []

    LOG.info(f"Found {len(files)} remote files (listing filtered by regex)")
    matched = [f for f in files if FNAME_RE.match(f)]
    LOG.info(f"Files matching target pattern: {len(matched)}")

    grouped = group_files_by_client(matched)
    latest = pick_latest_per_type(grouped)
    candidates = apply_rule1(latest)
    final_files = apply_rule2(candidates)

    if not final_files:
        LOG.info("No files selected for download")
    else:
        LOG.info(f"Selected {len(final_files)} files to download")

    downloaded = []
    for fname in final_files:
        remote_path = fname if args.remote_folder in ('.', '/') else f"{args.remote_folder.rstrip('/')}/{fname}"
        local_path = os.path.join(download_dir, fname)
        ok = download_remote_file(sftp, remote_path, local_path)
        if ok:
            downloaded.append(local_path)

    # Close SFTP
    try:
        if sftp:
            sftp.close()
        if client:
            client.close()
    except Exception:
        pass

    # Get Dropbox token once for all operations
    token = get_dropbox_token()
    if not token:
        LOG.error("Cannot proceed without Dropbox authentication")
        return 1
    
    # Upload to Dropbox under app folder path /cred/<filename>
    # First, remove all existing files in the Dropbox target folder, then upload new files.
    LOG.info(f"Wiping Dropbox folder before upload: {DROPBOX_TARGET_FOLDER}")
    wiped = delete_all_in_dropbox_folder(DROPBOX_TARGET_FOLDER, token)
    if not wiped:
        LOG.warning("Failed to wipe Dropbox folder; continuing to upload (files may be appended)")

    # First pass: try to upload all files (skip failures)
    LOG.info(f"\n{'='*60}")
    LOG.info("UPLOADING TO DROPBOX (FIRST PASS)")
    LOG.info(f"{'='*60}")
    
    failed_uploads = []
    uploaded_successfully = []
    
    for local in downloaded:
        filename = os.path.basename(local)
        dropbox_path = os.path.join(DROPBOX_TARGET_FOLDER, filename).replace('\\', '/')
        # Add small delay between uploads to avoid burst rate limits
        time.sleep(0.5)
        success = upload_to_dropbox(local, dropbox_path, token, attempts=1, skip_on_failure=True)
        if success:
            uploaded_successfully.append(local)
        else:
            failed_uploads.append((local, dropbox_path))
    
    # Retry failed uploads up to 3 times
    if failed_uploads:
        LOG.info(f"\n{'='*60}")
        LOG.info(f"RETRYING FAILED UPLOADS ({len(failed_uploads)} files)")
        LOG.info(f"{'='*60}")
        
        for retry_round in range(1, 4):
            if not failed_uploads:
                break
            LOG.info(f"\n🔄 Retry round {retry_round}/3 for {len(failed_uploads)} files...")
            time.sleep(2)  # Wait before retry
            
            still_failed = []
            for local, dropbox_path in failed_uploads:
                time.sleep(1.0)  # Longer delay for retries
                success = upload_to_dropbox(local, dropbox_path, token, attempts=1, skip_on_failure=True)
                if success:
                    uploaded_successfully.append(local)
                else:
                    still_failed.append((local, dropbox_path))
            failed_uploads = still_failed
    
    # Delete local files if requested and upload was successful
    if args.delete_after_upload and uploaded_successfully:
        LOG.info(f"\n🗑️  Deleting {len(uploaded_successfully)} successfully uploaded local files...")
        for local in uploaded_successfully:
            try:
                os.remove(local)
                LOG.info(f"   Deleted: {os.path.basename(local)}")
            except Exception as e:
                LOG.warning(f"   Failed to delete {os.path.basename(local)}: {e}")
    
    # Final summary
    LOG.info(f"\n{'='*60}")
    LOG.info("UPLOAD SUMMARY")
    LOG.info(f"{'='*60}")
    LOG.info(f"✅ Successfully uploaded: {len(uploaded_successfully)} files")
    if failed_uploads:
        LOG.error(f"❌ Failed uploads: {len(failed_uploads)} files")
        LOG.error("Failed account numbers:")
        for local, _ in failed_uploads:
            filename = os.path.basename(local)
            match = FNAME_RE.match(filename)
            if match:
                account = match.group('cid')
                file_type = match.group('type')
                LOG.error(f"   Account {account} - {file_type} ({filename})")
    else:
        LOG.info("✅ All files uploaded successfully!")
    LOG.info(f"{'='*60}\n")
    LOG.info('Finished')
    
    return 0 if not failed_uploads else 1

if __name__ == '__main__':
    main()
