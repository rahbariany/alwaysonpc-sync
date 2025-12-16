#!/usr/bin/env python3
"""
integrated_sync_enhanced.py

Complete sync solution that:
1. Downloads Credinvest SFTP files with retry logic
2. Uploads to Dropbox with rate limit handling
3. Fetches latest Vestr fee data via GraphQL
4. Aggregates and stores fee summaries in PostgreSQL
5. Populates fee snapshots for dashboard visualization
6. Provides comprehensive file logging with rotation

Usage:
  python integrated_sync_enhanced.py [options]

Options:
  --download-dir PATH        Local folder for Credinvest downloads
  --skip-credinvest          Skip Credinvest SFTP sync
  --skip-vestr-fees          Skip Vestr fee sync
  --skip-fee-snapshots       Skip fee snapshot population
  --delete-after-upload      Delete local files after successful Dropbox upload
  --log-file PATH            Custom log file path (default: logs/integrated_sync.log)
"""
import sys
import os
import logging
import argparse
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Import modules
try:
    from credinvest_sync import main as credinvest_main
except ImportError:
    credinvest_main = None

try:
    from vestr_fees_lightweight import sync_fees_dataset
except ImportError:
    sync_fees_dataset = None

try:
    from populate_fee_snapshots import populate_snapshots
except ImportError:
    populate_snapshots = None

try:
    from database_models import ensure_fee_tables, get_session
except ImportError:
    ensure_fee_tables = None
    get_session = None


def setup_logging(log_file=None, console_level=logging.INFO):
    """Setup comprehensive logging with file rotation and console output"""
    
    # Create logs directory if needed
    if log_file is None:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f'integrated_sync_{datetime.now().strftime("%Y%m%d")}.log')
    else:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
    
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Capture everything
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # File handler with rotation (10MB max, keep 5 backups)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler (less verbose)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    return log_file


def run_integrated_sync(args):
    """Run the complete sync process with comprehensive error handling"""
    
    logger = logging.getLogger(__name__)
    
    print("=" * 80)
    print("INTEGRATED SYNC - AlwaysOnPC Enhanced Edition")
    print("=" * 80)
    logger.info("Starting integrated sync session")
    logger.info(f"Arguments: {vars(args)}")
    
    success_count = 0
    total_tasks = 0
    errors = []
    
    # Task 1: Credinvest SFTP sync
    if not args.skip_credinvest:
        total_tasks += 1
        print("\n[1/3] Running Credinvest SFTP sync...")
        print("-" * 80)
        logger.info("=== TASK 1: Credinvest SFTP Sync ===")
        try:
            if credinvest_main is None:
                raise ImportError("credinvest_sync module not available")
            
            # Override sys.argv for credinvest_main
            original_argv = sys.argv
            sys.argv = ['credinvest_sync.py']
            if args.download_dir:
                sys.argv.extend(['--download-dir', args.download_dir])
            if args.delete_after_upload:
                sys.argv.append('--delete-after-upload')
            
            result = credinvest_main()
            sys.argv = original_argv
            
            if result == 0:
                logger.info("[SUCCESS] Credinvest sync completed successfully")
                print("[SUCCESS] Credinvest sync completed")
                success_count += 1
            else:
                logger.warning("[WARNING] Credinvest sync completed with errors")
                print("[WARNING] Credinvest sync completed with errors")
                errors.append("Credinvest sync: some uploads failed")
        except Exception as e:
            logger.error(f"[ERROR] Credinvest sync failed: {e}", exc_info=True)
            print(f"[ERROR] Credinvest sync failed: {e}")
            errors.append(f"Credinvest sync: {str(e)}")
    else:
        print("\n[1/3] Skipping Credinvest sync (--skip-credinvest)")
        logger.info("Skipping Credinvest SFTP sync (user requested)")
    
    # Task 2: Vestr fee data sync
    if not args.skip_vestr_fees:
        total_tasks += 1
        print("\n[2/3] Running Vestr fee data sync...")
        print("-" * 80)
        logger.info("=== TASK 2: Vestr Fee Data Sync ===")
        try:
            if sync_fees_dataset is None:
                raise ImportError("vestr_fees_lightweight module not available")
            if ensure_fee_tables is None:
                raise ImportError("database_models module not available")
            
            # Ensure database tables exist
            logger.info("Ensuring fee tables exist in database...")
            ensure_fee_tables()
            
            # Run incremental sync (fetch latest data)
            logger.info("Starting incremental fee sync from Vestr...")
            result = sync_fees_dataset(force_full=False)
            
            if result:
                logger.info("[SUCCESS] Vestr fee sync completed successfully")
                print("[SUCCESS] Vestr fee sync completed")
                success_count += 1
            else:
                logger.warning("[WARNING] Vestr fee sync completed with warnings")
                print("[WARNING] Vestr fee sync completed with warnings")
                errors.append("Vestr fee sync: completed with warnings")
        except Exception as e:
            logger.error(f"[ERROR] Vestr fee sync failed: {e}", exc_info=True)
            print(f"[ERROR] Vestr fee sync failed: {e}")
            errors.append(f"Vestr fee sync: {str(e)}")
    else:
        print("\n[2/3] Skipping Vestr fee sync (--skip-vestr-fees)")
        logger.info("Skipping Vestr fee data sync (user requested)")
    
    # Task 3: Fee snapshot population
    if not args.skip_fee_snapshots:
        total_tasks += 1
        print("\n[3/3] Running fee snapshot population...")
        print("-" * 80)
        logger.info("=== TASK 3: Fee Snapshot Population ===")
        try:
            if populate_snapshots is None:
                raise ImportError("populate_fee_snapshots module not available")
            
            # Run snapshot population
            logger.info("Populating fee snapshots from aggregated data...")
            populate_snapshots()
            
            logger.info("[SUCCESS] Fee snapshot population completed successfully")
            print("[SUCCESS] Fee snapshot population completed")
            success_count += 1
        except Exception as e:
            logger.error(f"[ERROR] Fee snapshot population failed: {e}", exc_info=True)
            print(f"[ERROR] Fee snapshot population failed: {e}")
            errors.append(f"Fee snapshot population: {str(e)}")
    else:
        print("\n[3/3] Skipping fee snapshot population (--skip-fee-snapshots)")
        logger.info("Skipping fee snapshot population (user requested)")
    
    # Final summary
    print("\n" + "=" * 80)
    print("INTEGRATED SYNC SUMMARY")
    print("=" * 80)
    print(f"Total tasks: {total_tasks}")
    print(f"[SUCCESS] Successful: {success_count}")
    print(f"[FAILED] Failed: {len(errors)}")
    
    logger.info("=== SYNC SESSION SUMMARY ===")
    logger.info(f"Total tasks: {total_tasks}, Successful: {success_count}, Failed: {len(errors)}")
    
    if errors:
        print("\n[ERRORS] Errors encountered:")
        for error in errors:
            print(f"   - {error}")
            logger.error(f"Summary error: {error}")
    else:
        print("\n[SUCCESS] ALL TASKS COMPLETED SUCCESSFULLY!")
        logger.info("[SUCCESS] All tasks completed successfully")
    
    print("=" * 80)
    logger.info("Integrated sync session ended")
    
    return 0 if success_count == total_tasks else 1


def main():
    parser = argparse.ArgumentParser(
        description='Integrated sync for Credinvest files, Vestr fees, and database snapshots'
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
        '--skip-vestr-fees',
        action='store_true',
        help='Skip Vestr fee data sync'
    )
    parser.add_argument(
        '--skip-fee-snapshots',
        action='store_true',
        help='Skip fee snapshot population'
    )
    parser.add_argument(
        '--delete-after-upload',
        action='store_true',
        help='Delete local files after successful Dropbox upload'
    )
    parser.add_argument(
        '--log-file',
        default=None,
        help='Custom log file path (default: logs/integrated_sync_YYYYMMDD.log)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose console output (DEBUG level)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    console_level = logging.DEBUG if args.verbose else logging.INFO
    log_file = setup_logging(log_file=args.log_file, console_level=console_level)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Log file: {log_file}")
    print(f"Logging to: {log_file}\n")
    
    try:
        exit_code = run_integrated_sync(args)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("Sync interrupted by user")
        print("\n[WARNING] Sync interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Unexpected error in main: {e}", exc_info=True)
        print(f"\n[ERROR] Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
