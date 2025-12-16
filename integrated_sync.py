#!/usr/bin/env python3
"""
integrated_sync.py

Combined script that:
1. Runs Credinvest SFTP sync to download and upload files to Dropbox
2. Populates fee snapshots from the Vestr fee database

This is the main script to be compiled into an executable for AlwaysOnPC.

Usage:
  python integrated_sync.py [--download-dir PATH] [--skip-credinvest] [--skip-fees]
"""
import sys
import argparse
import logging

# Import the existing credinvest sync functionality
from credinvest_sync import main as credinvest_main, LOG as cred_log

# Import fee snapshot population
from populate_fee_snapshots import populate_snapshots

# Import database setup
from database_models import ensure_fee_tables

LOG = logging.getLogger("integrated_sync")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run_integrated_sync(args):
    """
    Run the complete sync process:
    1. Credinvest SFTP sync (if enabled)
    2. Fee snapshot population (if enabled)
    """
    print("=" * 80)
    print("INTEGRATED SYNC - AlwaysOnPC")
    print("=" * 80)
    
    success = True
    
    # Step 1: Credinvest SFTP sync
    if not args.skip_credinvest:
        print("\n[1/2] Running Credinvest SFTP sync...")
        print("-" * 80)
        try:
            # Temporarily override sys.argv for credinvest_main
            original_argv = sys.argv
            sys.argv = ['credinvest_sync.py']
            if args.download_dir:
                sys.argv.extend(['--download-dir', args.download_dir])
            
            result = credinvest_main()
            sys.argv = original_argv
            
            if result != 0:
                LOG.warning("Credinvest sync returned non-zero exit code")
                success = False
            else:
                print("✅ Credinvest sync completed")
        except Exception as e:
            LOG.error(f"Credinvest sync failed: {e}")
            import traceback
            traceback.print_exc()
            success = False
    else:
        print("\n[1/2] Skipping Credinvest sync (--skip-credinvest)")
    
    # Step 2: Fee snapshot population
    if not args.skip_fees:
        print("\n[2/2] Running fee snapshot population...")
        print("-" * 80)
        try:
            # Ensure database tables exist
            ensure_fee_tables()
            
            # Run snapshot population
            populate_snapshots()
            print("✅ Fee snapshot population completed")
        except Exception as e:
            LOG.error(f"Fee snapshot population failed: {e}")
            import traceback
            traceback.print_exc()
            success = False
    else:
        print("\n[2/2] Skipping fee snapshot population (--skip-fees)")
    
    # Final status
    print("\n" + "=" * 80)
    if success:
        print("✅ INTEGRATED SYNC COMPLETE - ALL TASKS SUCCESSFUL")
    else:
        print("⚠️  INTEGRATED SYNC COMPLETE - SOME TASKS FAILED (see logs above)")
    print("=" * 80)
    
    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser(
        description='Integrated sync for Credinvest files and Vestr fee snapshots'
    )
    parser.add_argument(
        '--download-dir', '-d',
        default=None,
        help='Local folder to save downloaded Credinvest files (default: ./download)'
    )
    parser.add_argument(
        '--skip-credinvest',
        action='store_true',
        help='Skip Credinvest SFTP sync'
    )
    parser.add_argument(
        '--skip-fees',
        action='store_true',
        help='Skip fee snapshot population'
    )
    
    args = parser.parse_args()
    
    exit_code = run_integrated_sync(args)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
