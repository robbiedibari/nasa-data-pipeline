# NASA Asteroid Data Pipeline

A production-grade data engineering pipeline that extracts Near-Earth Object (NEO) data from NASA's API, transforms it into structured records, and loads it into a PostgreSQL database for analysis and machine learning applications.

## 📋 Project Overview

This pipeline automatically fetches asteroid close approach data from NASA's NeoWs (Near Earth Object Web Service) API and stores it in a relational database. The system handles rate limiting, data validation, and duplicate prevention while processing decades of historical asteroid observations.

### Key Features

- **Automated ETL Pipeline**: Extract, transform, and load asteroid data systematically
- **Historical Backfilling**: Load years of historical data in manageable 7-day chunks
- **Rate Limit Handling**: Respects NASA API limits (1,000 requests/hour)
- **Duplicate Prevention**: Unique constraints prevent data duplication
- **Scalable Architecture**: Designed to handle millions of records efficiently

## 🛠️ Tech Stack

- **Python 3.12+**: Core programming language
- **PostgreSQL 16**: Relational database
- **Docker & Docker Compose**: Containerized database infrastructure
- **SQLAlchemy**: ORM for database operations
- **pgAdmin**: Database management interface
- **NASA NeoWs API**: Data source

## 📁 Project Structure
```
nasa-data-pipeline/
├── asteroid_pipeline.py    # Main ETL pipeline script
├── database.py             # SQLAlchemy models and DB configuration
├── docker-compose.yml      # Docker services configuration
├── .env                    # Environment variables (not committed)
├── .gitignore             # Git ignore rules
├── requirements.txt        # Python dependencies
└── README.md              # Project documentation
```

## 🚀 Getting Started

### Prerequisites

- Python 3.12 or higher
- Docker Desktop
- NASA API Key ([Get one free here](https://api.nasa.gov/))

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/robbiedibari/nasa-data-pipeline
cd nasa-data-pipeline
```

2. **Set up environment variables**

Create a `.env` file in the project root:
```env
# Database Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=nasa
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# pgAdmin Configuration
PGADMIN_EMAIL=your_email@example.com
PGADMIN_PASSWORD=admin_password

# NASA API
NASA_KEY=your_nasa_api_key_here
```

3. **Install Python dependencies**
```bash
pip install -r requirements.txt
```

4. **Start the database infrastructure**
```bash
docker-compose up -d
```

5. **Initialize the database schema**
```bash
python database.py
```

6. **Verify database connection**

Access pgAdmin at `http://localhost:5050`
- Login with credentials from `.env`
- Add server connection:
  - Host: `postgres`
  - Port: `5432`
  - Database: `nasa`
  - Username/Password: from `.env`

## 💻 Usage

### Run the Pipeline

**Load recent data (last 5 years):**
```bash
python asteroid_pipeline.py
```

**Customize the date range** by editing `asteroid_pipeline.py`:
```python
if __name__ == "__main__":
    # Load specific date range
    backfill_asteroids("2020-01-01", "2025-11-10")
```

### Pipeline Execution

The pipeline will:
1. Fetch data from NASA API in 7-day chunks
2. Parse nested JSON into flat records
3. Insert data into PostgreSQL
4. Skip duplicates automatically
5. Display progress updates every 10 batches


## 📊 Database Schema

### `asteroid_approaches` Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | Integer (PK) | Auto-incrementing primary key |
| `neo_reference_id` | String(50) | NASA's unique asteroid identifier |
| `name` | String(255) | Asteroid designation |
| `close_approach_date` | Date | Date of closest approach to Earth |
| `absolute_magnitude_h` | Float | Absolute magnitude (brightness) |
| `estimated_diameter_km_min` | Float | Minimum estimated diameter (km) |
| `estimated_diameter_km_max` | Float | Maximum estimated diameter (km) |
| `is_potentially_hazardous` | Boolean | Potentially Hazardous Asteroid flag |
| `relative_velocity_kmh` | Float | Velocity relative to Earth (km/h) |
| `miss_distance_km` | Float | Distance from Earth (km) |
| `miss_distance_astronomical` | Float | Distance in AU |
| `orbiting_body` | String(50) | Body being orbited (typically Earth) |
| `ingested_at` | DateTime | Timestamp of data ingestion |


## 🎯 Future Enhancements

- [ ] Add automated daily/weekly incremental updates
- [ ] Implement checkpointing for pipeline resumption
- [ ] Add comprehensive logging system
- [ ] Build Apache Airflow DAG for orchestration
- [ ] Create data quality validation tests
- [ ] Add dbt for SQL transformations
- [ ] Develop machine learning models for approach prediction
- [ ] Build visualization dashboard (Streamlit/Grafana)

## 📈 Performance Metrics

- **API Rate Limit**: 1,000 requests/hour
- **Batch Size**: 7 days per request
- **Processing Speed**: ~4 seconds per batch
- **Historical Load Time** (2000-2025): ~90 minutes
- **Expected Records**: 50,000+ asteroid approaches

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- NASA's Near Earth Object Web Service (NeoWs) API
- [NASA API Portal](https://api.nasa.gov/)
- JPL's Center for Near-Earth Object Studies

## 📧 Contact

Your Name - [@yourtwitter](https://twitter.com/yourtwitter)

Project Link: [https://github.com/robbiedibari/nasa-data-pipeline](https://github.com/robbiedibari/nasa-data-pipeline)

---

**Note**: This is a personal learning project for data engineering practice. Always respect NASA API rate limits and terms of service.
