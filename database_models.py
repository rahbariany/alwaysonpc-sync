"""
Database models for Vestr fee tracking
Extracted from aisrender project for standalone use
"""

from datetime import datetime, date
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Date, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

Base = declarative_base()


class VestrFeeRecord(Base):
    """Persistent store for Vestr fee deductions fetched via GraphQL."""

    __tablename__ = 'vestr_fee_records'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fee_id = Column(String(64), unique=True, nullable=False, index=True)
    product_uid = Column(String(64))
    product_name = Column(String(255))
    product_isin = Column(String(32))
    currency = Column(String(10))
    fee_type = Column(String(64), index=True)
    fee_name = Column(String(255))
    beneficiary_id = Column(String(64), index=True)
    outstanding_quantity = Column(Float)
    position_change = Column(Float)
    amount_abs = Column(Float)
    booking_datetime = Column(DateTime, nullable=False)
    booking_date = Column(Date, nullable=False, index=True)
    raw_payload = Column(Text)
    synced_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_fee_booking_date_type', 'booking_date', 'fee_type'),
        Index('idx_fee_product_name', 'product_name'),
    )


class VestrFeeMonthlySummary(Base):
    """Aggregated monthly sums for Vestr fees (permanent storage)."""
    __tablename__ = 'vestr_fee_monthly_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    month = Column(String(7), nullable=False, index=True)  # YYYY-MM
    product_isin = Column(String(32), nullable=True, index=True)
    product_name = Column(String(255), nullable=True)
    fee_type = Column(String(64), nullable=True, index=True)
    fee_name = Column(String(255), nullable=True)
    currency = Column(String(10), nullable=True)
    sum_amount = Column(Float, default=0.0)
    sum_abs = Column(Float, default=0.0)
    record_count = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_fee_month_product_type', 'month', 'product_isin', 'fee_type', unique=True),
    )


class VestrFeeDailySummary(Base):
    """Daily totals for recent days (for 'today' visualization)."""
    __tablename__ = 'vestr_fee_daily_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    booking_date = Column(Date, nullable=False, index=True)
    product_isin = Column(String(32), nullable=True, index=True)
    product_name = Column(String(255), nullable=True)
    fee_type = Column(String(64), nullable=True, index=True)
    fee_name = Column(String(255), nullable=True)
    currency = Column(String(10), nullable=True)
    sum_amount = Column(Float, default=0.0)
    sum_abs = Column(Float, default=0.0)
    record_count = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_fee_daily_product_type', 'booking_date', 'product_isin', 'fee_type', unique=True),
    )


class VestrFeeProductTotal(Base):
    """Lifetime totals per product (for product-level visualization)."""
    __tablename__ = 'vestr_fee_product_totals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_isin = Column(String(32), nullable=True, index=True)
    product_name = Column(String(255), nullable=True)
    fee_type = Column(String(64), nullable=True, index=True)
    currency = Column(String(10), nullable=True)
    total_amount = Column(Float, default=0.0)
    total_abs = Column(Float, default=0.0)
    record_count = Column(Integer, default=0)
    first_booking_date = Column(Date, nullable=True)
    last_booking_date = Column(Date, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_fee_product_total', 'product_isin', 'fee_type', unique=True),
    )


class FeeLatestSnapshot(Base):
    """
    Latest snapshot of fee records per product - STORAGE OPTIMIZED
    Stores only the most recent record for each product to show current state
    """
    __tablename__ = 'fee_latest_snapshot'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_isin = Column(String(32), nullable=False, unique=True, index=True)
    product_name = Column(String(255))
    
    # Latest fee information
    last_mgmt_fee_date = Column(Date)
    last_mgmt_fee_amount = Column(Float)
    
    last_perf_fee_date = Column(Date)
    last_perf_fee_amount = Column(Float)
    
    last_custody_fee_date = Column(Date)
    last_custody_fee_amount = Column(Float)
    
    # Most recent activity
    last_fee_date = Column(Date, index=True)
    last_fee_type = Column(String(64))
    last_fee_amount = Column(Float)
    
    # Currency
    currency = Column(String(10))
    
    # Outstanding quantity (latest)
    outstanding_quantity = Column(Float)
    
    # Last updated tracking
    synced_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FeeSyncStatus(Base):
    """Track when the Vestr fees dataset was last synchronized."""

    __tablename__ = 'fee_sync_status'

    id = Column(Integer, primary_key=True, autoincrement=True)
    last_full_sync = Column(DateTime)
    last_incremental_sync = Column(DateTime)
    last_record_count = Column(Integer, default=0)
    last_seen_fee_id = Column(String(64))
    last_seen_booking_date = Column(Date)
    last_run_mode = Column(String(20))
    last_duration_seconds = Column(Float)
    status = Column(String(20), default='idle')
    last_error = Column(Text)
    last_sync_started_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def mark_sync(
        self,
        *,
        mode: str,
        record_count: int,
        latest_booking,
        last_fee_id,
        duration_seconds: float,
    ) -> None:
        now = datetime.utcnow()
        self.last_run_mode = mode
        self.last_duration_seconds = duration_seconds
        self.last_incremental_sync = now
        if mode == 'full':
            self.last_full_sync = now
        self.last_record_count = record_count
        self.last_seen_fee_id = last_fee_id
        self.last_seen_booking_date = latest_booking
        self.status = 'success'
        self.last_error = None
        self.updated_at = now

    def mark_failure(self, error_message: str) -> None:
        self.status = 'error'
        self.last_error = (error_message or '')[:2000]
        self.updated_at = datetime.utcnow()


# Database connection configuration
def get_database_url():
    """
    Get database URL from environment variable or use Render PostgreSQL
    """
    database_url = os.environ.get('DATABASE_URL')
    
    if not database_url:
        # Use Render external database URL (for connections from outside Render's network)
        database_url = "postgresql://amc_4s4m_user:yAIUXc8xIJdgjCeYEFBX0wrSb2wJJen4@dpg-d409arfdiees73ampgl0-a.oregon-postgres.render.com/amc_4s4m"
    
    return database_url


def get_engine():
    """
    Create SQLAlchemy engine for database connection with optimized pooling
    """
    database_url = get_database_url()
    
    engine = create_engine(
        database_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=180,
        pool_timeout=20,
        connect_args={
            'connect_timeout': 10,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5,
            'options': '-c statement_timeout=30000'
        }
    )
    
    return engine


def get_session():
    """
    Create a database session
    """
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


def ensure_fee_tables():
    """Create fee-related tables on-demand."""
    engine = get_engine()
    VestrFeeRecord.__table__.create(bind=engine, checkfirst=True)
    VestrFeeMonthlySummary.__table__.create(bind=engine, checkfirst=True)
    VestrFeeDailySummary.__table__.create(bind=engine, checkfirst=True)
    VestrFeeProductTotal.__table__.create(bind=engine, checkfirst=True)
    FeeSyncStatus.__table__.create(bind=engine, checkfirst=True)
    FeeLatestSnapshot.__table__.create(bind=engine, checkfirst=True)
