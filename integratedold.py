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
    --skip-fee-aggregation     Skip refreshing aggregated fee tables
  --delete-after-upload      Delete local files after successful Dropbox upload
    --ais-root PATH            Explicit path to ais-amc-automate repo for aggregation helpers
  --log-file PATH            Custom log file path (default: logs/integrated_sync.log)
"""
import sys
import os
import logging
import argparse
from datetime import datetime
from logging.handlers import RotatingFileHandler
import subprocess

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


def discover_and_add_project_root(explicit_root=None):
    """Try to discover the ais-amc-automate project root and add it to sys.path.

    If `explicit_root` is provided, use and validate it.
    Returns the path added or None.
    """
    here = os.path.dirname(os.path.abspath(__file__))

    # If user provided explicit root, prefer that
    if explicit_root:
        explicit_root = os.path.abspath(explicit_root)
        marker = os.path.join(explicit_root, 'app', 'processors', 'fee_aggregator.py')
        if os.path.exists(marker):
            if explicit_root not in sys.path:
                sys.path.insert(0, explicit_root)
            return explicit_root

    # Search Desktop siblings for ais-amc-automate folder specifically
    parent = os.path.abspath(os.path.join(here, '..'))
    try:
        for name in os.listdir(parent):
            potential = os.path.join(parent, name)
            # Prioritize folders containing 'ais-amc-automate'
            if 'ais-amc-automate' in potential.lower():
                marker = os.path.join(potential, 'app', 'processors', 'fee_aggregator.py')
                if os.path.exists(marker):
                    if potential not in sys.path:
                        sys.path.insert(0, potential)
                    return potential
    except Exception:
        pass

    # Common local layout: Desktop/"amc automate"/ais-amc-automate
    try:
        for name in os.listdir(parent):
            base = os.path.join(parent, name)
            nested = os.path.join(base, 'ais-amc-automate')
            marker = os.path.join(nested, 'app', 'processors', 'fee_aggregator.py')
            if os.path.exists(marker):
                if nested not in sys.path:
                    sys.path.insert(0, nested)
                return nested
    except Exception:
        pass

    # Fallback: any folder with fee_aggregator marker
    try:
        for name in os.listdir(parent):
            potential = os.path.join(parent, name)
            marker = os.path.join(potential, 'app', 'processors', 'fee_aggregator.py')
            if os.path.exists(marker) and 'aisrender' not in potential.lower():
                if potential not in sys.path:
                    sys.path.insert(0, potential)
                return potential
    except Exception:
        pass

    return None


def _has_vestr_credentials(logger: logging.Logger | None = None) -> bool:
    """Return True if the Vestr fee sync has credentials available.

    Primary source is env vars (recommended). As a fallback for legacy/local
    setups, some scrapers may embed defaults; in that case we allow the sync
    to proceed so it can self-validate.
    """

    user = os.environ.get('VESTR_USERNAME')
    pw = os.environ.get('VESTR_PASSWORD')
    if user and user.strip() and pw and pw.strip():
        return True

    # Fallback: if the underlying scraper has usable configured credentials.
    try:
        from vestr_lightweight import LightweightVestrScraper  # type: ignore

        scraper = LightweightVestrScraper()
        if getattr(scraper, 'username', None) and getattr(scraper, 'password', None):
            if logger:
                logger.warning(
                    "VESTR_USERNAME/VESTR_PASSWORD not set; proceeding with scraper-configured credentials. "
                    "Set env vars for production."
                )
            return True
    except Exception:
        pass

    return False


def run_fee_aggregation_task(project_root=None):
    """Run the FeeAggregator from ais-amc-automate to refresh summary tables."""

    root = discover_and_add_project_root(project_root)
    if not root:
        raise RuntimeError("Unable to locate ais-amc-automate project root for fee aggregation")

    try:
        from app.processors.fee_aggregator import FeeAggregator  # type: ignore
        from app.models.database import get_session  # type: ignore
    except ImportError as exc:
        raise RuntimeError(f"Failed to import FeeAggregator from {root}: {exc}") from exc

    session = get_session()
    try:
        aggregator = FeeAggregator()
        stats = aggregator.aggregate_new_data(session)
        return stats
    finally:
        session.close()


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

    project_root = discover_and_add_project_root(getattr(args, 'ais_root', None))
    if project_root:
        logger.info("Detected ais-amc-automate root at %s", project_root)
    else:
        logger.warning("Could not automatically locate ais-amc-automate; aggregation helpers may not run")
    
    success_count = 0
    total_tasks = 0
    errors = []
    aggregation_stats = None
    
    # Task 1: Credinvest SFTP sync
    if not args.skip_credinvest:
        total_tasks += 1
        print("\n[1/4] Running Credinvest SFTP sync...")
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
        print("\n[1/4] Skipping Credinvest sync (--skip-credinvest)")
        logger.info("Skipping Credinvest SFTP sync (user requested)")
    
    # Task 2: Vestr fee data sync
    if not args.skip_vestr_fees:
        total_tasks += 1
        print("\n[2/4] Running Vestr fee data sync...")
        print("-" * 80)
        logger.info("=== TASK 2: Vestr Fee Data Sync ===")
        try:
            if sync_fees_dataset is None:
                raise ImportError("vestr_fees_lightweight module not available")
            if ensure_fee_tables is None:
                raise ImportError("database_models module not available")

            if not _has_vestr_credentials(logger):
                raise RuntimeError(
                    "Missing Vestr credentials. Set VESTR_USERNAME and VESTR_PASSWORD environment variables "
                    "(or configure credentials in the scraper)."
                )
            
            # Ensure database tables exist
            logger.info("Ensuring fee tables exist in database...")
            ensure_fee_tables()

            # If user requested a full bootstrap, call the scraper directly.
            if getattr(args, 'full', False):
                logger.info("Starting FULL fee sync from Vestr (force_full=True)")
                result = sync_fees_dataset(force_full=True)
            else:
                # Prefer a targeted sync that fetches only records after the
                # latest booking date present in the DB. The helper
                # `run_sync_after_db_latest.py` performs that logic and is
                # located in the ais-amc-automate `scripts` folder. If the
                # helper is unavailable, fall back to the incremental
                # `sync_fees_dataset` behavior.
                helper_path = None
                if project_root:
                    helper_path = os.path.join(project_root, 'scripts', 'run_sync_after_db_latest.py')

                # If not found, try common nearby locations (Desktop sibling layout)
                if not helper_path or not os.path.exists(helper_path):
                    here = os.path.dirname(os.path.abspath(__file__))
                    parent = os.path.abspath(os.path.join(here, '..'))
                    candidates = [
                        os.path.join(parent, '..', 'amc automate', 'ais-amc-automate', 'scripts', 'run_sync_after_db_latest.py'),
                        os.path.join(parent, '..', 'ais-amc-automate', 'scripts', 'run_sync_after_db_latest.py'),
                        os.path.join(parent, 'amc automate', 'ais-amc-automate', 'scripts', 'run_sync_after_db_latest.py'),
                    ]
                    for cand in candidates:
                        cand = os.path.abspath(cand)
                        if os.path.exists(cand):
                            helper_path = cand
                            break

                if helper_path and os.path.exists(helper_path):
                    cmd = [sys.executable, helper_path]
                    logger.info("Running targeted backfill helper: %s", ' '.join(cmd))
                    run_env = None
                    # If Vestr credentials are not present in the environment, but the
                    # local lightweight scraper has credentials configured (legacy/local
                    # defaults), forward them to the helper subprocess so it can log in.
                    if not (os.environ.get('VESTR_USERNAME') and os.environ.get('VESTR_PASSWORD')):
                        try:
                            from vestr_lightweight import LightweightVestrScraper  # type: ignore

                            scraper = LightweightVestrScraper()
                            if getattr(scraper, 'username', None) and getattr(scraper, 'password', None):
                                run_env = os.environ.copy()
                                run_env['VESTR_USERNAME'] = str(scraper.username)
                                run_env['VESTR_PASSWORD'] = str(scraper.password)
                                if getattr(scraper, 'otp_secret', None):
                                    run_env['VESTR_OTP_SECRET'] = str(scraper.otp_secret)
                                if getattr(scraper, 'totp_time_offset_seconds', None) is not None:
                                    run_env['VESTR_TOTP_TIME_OFFSET_SECONDS'] = str(
                                        int(getattr(scraper, 'totp_time_offset_seconds') or 0)
                                    )
                        except Exception:
                            run_env = None

                    proc = subprocess.run(cmd, env=run_env)
                    result = {"subprocess_returncode": proc.returncode}
                else:
                    logger.info("Starting incremental fee sync from Vestr (fallback)")
                    result = sync_fees_dataset(force_full=False)

            ok = False
            if isinstance(result, dict) and "subprocess_returncode" in result:
                ok = int(result.get("subprocess_returncode") or 1) == 0
            else:
                ok = bool(result)

            if ok:
                logger.info("[SUCCESS] Vestr fee sync completed successfully: %s", result)
                print("[SUCCESS] Vestr fee sync completed")
                success_count += 1
            else:
                logger.warning("[WARNING] Vestr fee sync did not complete cleanly: %s", result)
                print("[ERROR] Vestr fee sync failed")
                errors.append(f"Vestr fee sync: failed ({result})")
        except Exception as e:
            logger.error(f"[ERROR] Vestr fee sync failed: {e}", exc_info=True)
            print(f"[ERROR] Vestr fee sync failed: {e}")
            errors.append(f"Vestr fee sync: {str(e)}")
    else:
        print("\n[2/4] Skipping Vestr fee sync (--skip-vestr-fees)")
        logger.info("Skipping Vestr fee data sync (user requested)")
    
    # Task 3: Fee aggregation
    if not getattr(args, 'skip_fee_aggregation', False):
        total_tasks += 1
        print("\n[3/4] Running fee aggregation...")
        print("-" * 80)
        logger.info("=== TASK 3: Fee Aggregation ===")
        try:
            stats = run_fee_aggregation_task(project_root)
            aggregation_stats = stats
            logger.info("Fee aggregation stats: %s", stats)
            print("[SUCCESS] Fee aggregation completed")
            success_count += 1
        except Exception as e:
            logger.error(f"[ERROR] Fee aggregation failed: {e}", exc_info=True)
            print(f"[ERROR] Fee aggregation failed: {e}")
            errors.append(f"Fee aggregation: {str(e)}")
    else:
        print("\n[3/4] Skipping fee aggregation (--skip-fee-aggregation)")
        logger.info("Skipping fee aggregation (user requested)")

    # Task 4: Fee snapshot population
    if not args.skip_fee_snapshots:
        skip_snapshot_due_to_fresh_data = (
            aggregation_stats is not None and
            aggregation_stats.get('raw_records_processed', 0) == 0 and
            aggregation_stats.get('snapshots_updated', 0) == 0
        )
        if skip_snapshot_due_to_fresh_data:
            total_tasks += 1
            print("\n[4/4] Skipping fee snapshot population (already current)")
            logger.info("Skipping fee snapshot population: aggregator reported no new data")
            success_count += 1
        else:
            total_tasks += 1
            print("\n[4/4] Running fee snapshot population...")
            print("-" * 80)
            logger.info("=== TASK 4: Fee Snapshot Population ===")
            try:
                if populate_snapshots is None:
                    raise ImportError("populate_fee_snapshots module not available")
                
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
        print("\n[4/4] Skipping fee snapshot population (--skip-fee-snapshots)")
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
        '--skip-fee-aggregation',
        action='store_true',
        help='Skip refreshing aggregated fee tables'
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
        '--ais-root',
        default=None,
        help='Explicit path to the ais-amc-automate repo when running cross-project helpers'
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
