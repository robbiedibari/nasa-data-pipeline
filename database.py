import os
from datetime import datetime,timezone

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    create_engine,
    func,
    CheckConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,Mapped, mapped_column

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(DATABASE_URL)

# Create engine:

# Base class for models
Base = declarative_base()

# Session factory:
SessionLocal = sessionmaker(bind=engine)

class AsteroidApproach(Base):
    __tablename__ = "asteroid_approaches"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Asteroid properties
    neo_reference_id = Column(String(50), nullable=False)
    name = Column(String(255), nullable=False)
    nasa_jpl_url = Column(String(500))
    absolute_magnitude_h = Column(Float)
    estimated_diameter_km_min = Column(Float)
    estimated_diameter_km_max = Column(Float)
    estimated_diameter_miles_min = Column(Float)
    estimated_diameter_miles_max = Column(Float)
    is_potentially_hazardous = Column(Boolean, default=False)
    is_sentry_object = Column(Boolean, default=False)

    # Close approach data
    close_approach_date = Column(Date, nullable=False)
    close_approach_date_full = Column(String(50))
    epoch_date_close_approach = Column(BigInteger)
    relative_velocity_kmh = Column(Float)
    relative_velocity_kms = Column(Float)
    relative_velocity_miles_per_hour = Column(Float)
    miss_distance_astronomical = Column(Float)
    miss_distance_lunar = Column(Float)
    miss_distance_km = Column(Float)
    miss_dstance_miles = Column(Float)
    orbiting_body = Column(String(50), default="Earth")

    # Metadata
    ingested_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index(
            "idx_unique_approach",
            "neo_reference_id",
            "close_approach_date",
            unique=True,
        ),
        Index("idx_approach_date", "close_approach_date"),
        Index("idx_neo_id", "neo_reference_id"),
    )

    def __repr__(self):
        return (
            f"<AsteroidApproach(name='{self.name}', date='{self.close_approach_date}')>"
        )

# Retrieving the latest asteroid approach date.
def get_latest_approach_date():
    with SessionLocal() as session:
        return session.query(
            func.max(AsteroidApproach.close_approach_date)).scalar()

# Table to monitor successfull and failed ingestion.
class IngestionRun(Base):
    __tablename__ = "ingestion_runs"
    id = Column(Integer, primary_key = True, autoincrement = True)
    requested_start_date = Column(Date, nullable=False)
    requested_end_date = Column(Date,nullable=False)
    started_at = Column(
        DateTime(timezone=True),
        nullable = False,
        default = lambda:datetime.now(timezone.utc)
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default = "running"
    )
    records_processed = Column(
        Integer,
        nullable = False,
        default = 0
    )

    error_message = Column(
        Text,
        nullable=True
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'succeeded', 'failed')",
            name="ck_ingestion_runs_status"
        ),
        CheckConstraint(
            "requested_end_date >= requested_start_date",
            name="ck_ingestion_runs_date_range",
        ),
        CheckConstraint(
            "records_processed >=0",
            name="ck_ingestion_runs_record_count",
        ),

    )
    # Receiving record of ranges pipeline completed.
    def start_ingestion_run(start_date, end_date):
        with SessionLocal() as session:
            run = IngestionRun(
                requested_start_date = start_date,
                requested_end_date = end_date,
            )
            session.add(run)
            session.commit()
            session.refresh(run)

            return run.id

    def complete_ingestion_run(run_id, records_processed):
        with SessionLocal() as session:
            run = session.get(IngestionRun, run_id)

            if run is None:
                raise ValueError(f'Ingestion run {run_id} does not exists')

            run.status = "succeeded"
            run.finished_at = datetime.now(timezone.utc)
            run.records_processed = records_processed

            session.commit()

    def fail_ingestion_run(run_id, error_message):
        with SessionLocal() as session:
            run = session.get(IngestionRun, run_id)

            if run is None:
                raise ValueError(f'Ingestion run {run_id} does not exists')

            run.status = "failed"
            run.finished_at = datetime.now(timezone.utc)
            run.error_message = error_message

            session.commit()





# Function to initialize database
def init_db():
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    init_db()
