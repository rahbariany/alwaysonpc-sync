"""
Populate fee_latest_snapshot table from existing aggregate data.
This script rebuilds the snapshot table to show latest fees per product.
Standalone version for AlwaysOnPC project.
"""
from datetime import datetime, date
from database_models import get_session, VestrFeeRecord, FeeLatestSnapshot


def populate_snapshots():
    """Populate fee_latest_snapshot from vestr_fee_records."""
    print("=" * 80)
    print("POPULATE FEE SNAPSHOTS")
    print("=" * 80)
    
    session = get_session()
    try:
        # First, check current state
        raw_count = session.query(VestrFeeRecord).count()
        snapshot_count_before = session.query(FeeLatestSnapshot).count()
        
        print(f"\n📊 Current State:")
        print(f"   Raw records: {raw_count}")
        print(f"   Snapshot records (before): {snapshot_count_before}")
        
        if raw_count == 0:
            print("\n⚠️  No raw records available - cannot populate snapshots")
            return
        
        # Get all products from raw records
        products_data = (
            session.query(
                VestrFeeRecord.product_name,
                VestrFeeRecord.product_isin
            )
            .distinct()
            .all()
        )
        
        print(f"\n🔧 Processing {len(products_data)} products...")
        
        # Clear existing snapshots (we'll rebuild from scratch)
        session.query(FeeLatestSnapshot).delete()
        session.commit()
        
        inserted = 0
        for product_name, isin in products_data:
            # Get latest management fee
            mgmt_latest = (
                session.query(VestrFeeRecord)
                .filter(
                    VestrFeeRecord.product_name == product_name,
                    VestrFeeRecord.fee_type == 'ManagementFeeDeduction'
                )
                .order_by(VestrFeeRecord.booking_date.desc())
                .first()
            )
            
            # Get latest performance fee
            perf_latest = (
                session.query(VestrFeeRecord)
                .filter(
                    VestrFeeRecord.product_name == product_name,
                    VestrFeeRecord.fee_type == 'PerformanceFeeDeduction'
                )
                .order_by(VestrFeeRecord.booking_date.desc())
                .first()
            )
            
            # Get latest custody fee
            custody_latest = (
                session.query(VestrFeeRecord)
                .filter(
                    VestrFeeRecord.product_name == product_name,
                    VestrFeeRecord.fee_type == 'CustodyFeeDeduction'
                )
                .order_by(VestrFeeRecord.booking_date.desc())
                .first()
            )
            
            # Determine the most recent fee overall
            last_fee_date = None
            last_fee_type = None
            last_fee_amount = None
            outstanding_qty = 0
            
            if mgmt_latest is not None and perf_latest is not None:
                # Extract booking_date values into local variables
                mgmt_bd = getattr(mgmt_latest, 'booking_date', None)
                perf_bd = getattr(perf_latest, 'booking_date', None)

                if mgmt_bd is not None and perf_bd is not None:
                    if mgmt_bd >= perf_bd:
                        last_fee_date = mgmt_bd
                        last_fee_type = 'ManagementFeeDeduction'
                        last_fee_amount = mgmt_latest.amount_abs
                        outstanding_qty = mgmt_latest.outstanding_quantity or 0
                    else:
                        last_fee_date = perf_bd
                        last_fee_type = 'PerformanceFeeDeduction'
                        last_fee_amount = perf_latest.amount_abs
                        outstanding_qty = perf_latest.outstanding_quantity or 0
                else:
                    # Fallback: prefer whichever booking_date is present
                    last_fee_date = mgmt_bd or perf_bd
                    mgmt_cmp = mgmt_bd if mgmt_bd is not None else date.min
                    perf_cmp = perf_bd if perf_bd is not None else date.min
                    last_fee_type = 'ManagementFeeDeduction' if mgmt_cmp >= perf_cmp else 'PerformanceFeeDeduction'
                    last_fee_amount = mgmt_latest.amount_abs or perf_latest.amount_abs
                    outstanding_qty = mgmt_latest.outstanding_quantity or perf_latest.outstanding_quantity or 0
            elif mgmt_latest is not None:
                last_fee_date = mgmt_latest.booking_date
                last_fee_type = 'ManagementFeeDeduction'
                last_fee_amount = mgmt_latest.amount_abs
                outstanding_qty = mgmt_latest.outstanding_quantity or 0
            elif perf_latest is not None:
                last_fee_date = perf_latest.booking_date
                last_fee_type = 'PerformanceFeeDeduction'
                last_fee_amount = perf_latest.amount_abs
                outstanding_qty = perf_latest.outstanding_quantity or 0

            # Check custody fee for overall last fee
            if custody_latest is not None:
                custody_bd = getattr(custody_latest, 'booking_date', None)
                if custody_bd is not None:
                    if last_fee_date is None or custody_bd > last_fee_date:
                        last_fee_date = custody_bd
                        last_fee_type = 'CustodyFeeDeduction'
                        last_fee_amount = custody_latest.amount_abs
                        outstanding_qty = custody_latest.outstanding_quantity or 0

            if last_fee_date is not None:  # Only create snapshot if we have at least one fee
                snapshot = FeeLatestSnapshot(
                    product_isin=isin or f'UNKNOWN_{product_name[:20]}',
                    product_name=product_name,
                    last_mgmt_fee_date=mgmt_latest.booking_date if mgmt_latest else None,
                    last_mgmt_fee_amount=mgmt_latest.amount_abs if mgmt_latest else None,
                    last_perf_fee_date=perf_latest.booking_date if perf_latest else None,
                    last_perf_fee_amount=perf_latest.amount_abs if perf_latest else None,
                    last_custody_fee_date=custody_latest.booking_date if custody_latest else None,
                    last_custody_fee_amount=custody_latest.amount_abs if custody_latest else None,
                    last_fee_date=last_fee_date,
                    last_fee_type=last_fee_type,
                    last_fee_amount=last_fee_amount,
                    outstanding_quantity=outstanding_qty,
                    currency='EUR'
                )
                session.add(snapshot)
                inserted += 1
        
        session.commit()
        
        # Verify results
        snapshot_count_after = session.query(FeeLatestSnapshot).count()
        
        print(f"\n✅ Snapshots Created:")
        print(f"   Inserted: {inserted}")
        print(f"   Total snapshot records: {snapshot_count_after}")
        
        # Show sample data
        print(f"\n📋 Sample Snapshots (first 5):")
        samples = session.query(FeeLatestSnapshot).limit(5).all()
        for s in samples:
            print(f"   {s.product_name[:30]:30} | Units: {s.outstanding_quantity:>10.2f} | Last: {s.last_fee_date}")
        
        print("\n" + "=" * 80)
        print("COMPLETE")
        print("=" * 80)
        
    except Exception as e:
        session.rollback()
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        session.close()


if __name__ == '__main__':
    populate_snapshots()
