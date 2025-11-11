import os

from dotenv import load_dotenv
from sqlalchemy import Column, Date, Integer, String, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = (
    f"postgres://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

# Create engine:
engine = create_engine(DATABASE_URL, echo=True)

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


# Function to initialize database
def init_db():
    Base.metadata.create_all(engine)
    print("Database tables created successfully!")


if __name__ == "__main__":
    init_db()
