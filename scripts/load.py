import duckdb
import os
import logging
import time

# Make sure logs/ folder exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    filename="logs/load.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level="DEBUG"
)

logger = logging.getLogger(__name__)


def load_parquet_files():
    print("load_parquet_files() has started")

    con = None

    # Sample data links for reference:
    # green: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet
    # yellow: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

    try:
        logger.info("Starting data load process")

        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Enable httpfs for remote parquet file reading
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        logger.info("Installed and loaded httpfs extension")

        # Drop tables if they already exist (fresh load each time)
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_trips;
            DROP TABLE IF EXISTS green_trips;
            DROP TABLE IF EXISTS emissions;
        """)
        logger.info("Dropped tables if existed: yellow_trips, green_trips, emissions")

        # Load yellow trip data (2015–2024)
        yellow_total = 0
        yellow_table_created = False

        for year in range(2015, 2025):
            year_str = f"{year}"
            for month in range(1, 13):
                month_str = f"{month:02d}"
                file_path = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_str}-{month_str}.parquet"
                logger.info(f"Loading yellow trip data from {year_str}-{month_str}")

                try:
                    # Create table on first file, insert on later ones
                    if not yellow_table_created:
                        con.execute(f"""
                            CREATE TABLE yellow_trips AS
                            SELECT 
                                vendorid,
                                tpep_pickup_datetime,
                                tpep_dropoff_datetime,
                                passenger_count,
                                trip_distance
                            FROM read_parquet('{file_path}');
                        """)
                        yellow_table_created = True
                        logger.info("Created table yellow_trips")

                    else:
                        con.execute(f"""
                            INSERT INTO yellow_trips
                            SELECT 
                                vendorid,
                                tpep_pickup_datetime,
                                tpep_dropoff_datetime,
                                passenger_count,
                                trip_distance
                            FROM read_parquet('{file_path}');
                        """)
                        logger.info(f"Inserted data into yellow_trips from {file_path}")

                    # Track growing record count
                    count = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
                    yellow_total = count
                    logger.info(f"Total records in yellow_trips after loading {file_path}: {yellow_total}")
                    print(f"Total records in yellow_trips after loading {year_str}-{month_str}: {yellow_total}")

                except Exception as e:
                    print(f"[Yellow] Skipping {year_str}-{month_str} ({e})")
                    logger.warning(f"Yellow trip data missing or failed: {file_path}")
                time.sleep(15)  # Pause to avoid hammering the server

        # Load green trip data (2015–2024)
        green_total = 0
        green_table_created = False

        for year in range(2015, 2025):
            year_str = f"{year}"
            for month in range(1, 13):
                month_str = f"{month:02d}"
                file_path = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year_str}-{month_str}.parquet"
                logger.info(f"Loading green trip data from {year_str}-{month_str}")

                try:
                    # Create table on first file, insert on later ones
                    if not green_table_created:
                        con.execute(f"""
                            CREATE TABLE green_trips AS
                            SELECT 
                                vendorid,
                                lpep_pickup_datetime,
                                lpep_dropoff_datetime,
                                passenger_count,
                                trip_distance
                            FROM read_parquet('{file_path}');
                        """)
                        green_table_created = True
                        logger.info("Created table green_trips")

                    else:
                        con.execute(f"""
                            INSERT INTO green_trips
                            SELECT 
                                vendorid,
                                lpep_pickup_datetime,
                                lpep_dropoff_datetime,
                                passenger_count,
                                trip_distance
                            FROM read_parquet('{file_path}');
                        """)
                        logger.info(f"Inserted data into green_trips from {file_path}")

                    # Track growing record count
                    count = con.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
                    green_total = count
                    logger.info(f"Total records in green_trips after loading {file_path}: {green_total}")
                    print(f"Total records in green_trips after loading {year_str}-{month_str}: {green_total}")

                except Exception as e:
                    print(f"[Green] Skipping {year_str}-{month_str} ({e})")
                    logger.warning(f"Green trip data missing or failed: {file_path}")
                time.sleep(15)

        # Load emissions data from csv

        emissions_path = os.path.join("data", "vehicle_emissions.csv")
        if not os.path.exists(emissions_path):
            raise FileNotFoundError(f"Emissions data file not found at {emissions_path}")

        con.execute(f"""
            CREATE TABLE emissions AS
            SELECT * FROM read_csv_auto('{emissions_path}', header=True);
        """)
        logger.info("Created table emissions from local CSV file")

        # Verify emissions row count
        count = con.execute("SELECT COUNT(*) FROM emissions").fetchone()[0]
        print(f"Total records in emissions table: {count}")
        logger.info(f"Total records in emissions table: {count}")

        # Final totals + basic summaries
        yellow_total = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]
        green_total = con.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]
        emissions_total = con.execute("SELECT COUNT(*) FROM emissions").fetchone()[0]

        print("Final Counts")
        print(f"Yellow trips: {yellow_total:,}")
        print(f"Green trips: {green_total:,}")
        print(f"Emissions records: {emissions_total:,}")

        # Compute basic descriptive stats
        print("Basic Summaries:")
        avg_yellow_distance = con.execute("SELECT AVG(trip_distance) FROM yellow_trips").fetchone()[0]
        total_yellow_passengers = con.execute("SELECT SUM(passenger_count) FROM yellow_trips").fetchone()[0]

        avg_green_distance = con.execute("SELECT AVG(trip_distance) FROM green_trips").fetchone()[0]
        total_green_passengers = con.execute("SELECT SUM(passenger_count) FROM green_trips").fetchone()[0]

        print(f"Avg Yellow trip distance: {avg_yellow_distance:.2f}")
        print(f"Total Yellow passengers: {total_yellow_passengers:,}")
        print(f"Avg Green trip distance: {avg_green_distance:.2f}")
        print(f"Total Green passengers: {total_green_passengers:,}")

        logger.info(f"Avg Yellow trip distance: {avg_yellow_distance:.2f}")
        logger.info(f"Total Yellow passengers: {total_yellow_passengers:,}")
        logger.info(f"Avg Green trip distance: {avg_green_distance:.2f}")
        logger.info(f"Total Green passengers: {total_green_passengers:,}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    load_parquet_files()
