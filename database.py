import os
from datetime import datetime

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
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"

engine = create_engine(DATABASE_URL, echo=True)

# Create engine:

# Base class for models
Base = declarative_base()

# Session factory:
SessionLocal = sessionmaker(bind=engine)


class APOD(Base):
    __tablename__ = "apod"

    date = Column(Date, primary_key=True)
    title = Column(String(255), nullable=False)
    explanation = Column(Text)
    url = Column(String(500))
    media_type = Column(String(50))
    hdurl = Column(String(500))

    def __repr__(self):
        return f"<APOD(date='{self.date},title='{self.title}')>"


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
    is_potentially_hazardous = Column(Boolean, default=False)
    is_sentry_object = Column(Boolean, default=False)

    # Close approach data
    close_approach_date = Column(Date, nullable=False)
    close_approach_date_full = Column(String(50))
    epoch_date_close_approach = Column(BigInteger)
    relative_velocity_kmh = Column(Float)
    relative_velocity_kms = Column(Float)
    miss_distance_km = Column(Float)
    miss_distance_astronomical = Column(Float)
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


# Function to initialize database
def init_db():
    Base.metadata.create_all(engine)
    print("✓ Database tables created successfully!")
    print("  - apod")
    print("  - asteroid_approaches")


if __name__ == "__main__":
    init_db()
