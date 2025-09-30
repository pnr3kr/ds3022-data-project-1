import duckdb
import logging

# Configure logging for cleaning process
logging.basicConfig(
    filename="logs/clean.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level="DEBUG"
)

logger = logging.getLogger(__name__)


def clean_trips():
    con = None
    try:
        # Connect to DuckDB database file
        con = duckdb.connect(database='emissions.duckdb', read_only=False)

        logger.info("Connected to DuckDB for cleaning")
        print("Started data cleaning process")

        # Remove duplicates from yellow_trips
        # NOTE: This section is commented out due to local machine constraints, but logic is included
        '''
        yellow_before_dupes = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]

        con.execute("""
            CREATE OR REPLACE TABLE yellow_trips_clean AS
            SELECT * EXCLUDE rn
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            vendorid,
                            tpep_pickup_datetime,
                            tpep_dropoff_datetime,
                            passenger_count,
                            trip_distance
                        ORDER BY tpep_pickup_datetime
                    ) AS rn
                FROM yellow_trips
            )
            WHERE rn = 1;
        """)

        yellow_after_dupes = con.execute("SELECT COUNT(*) FROM yellow_trips_clean").fetchone()[0]
        yellow_removed_dupes = yellow_before_dupes - yellow_after_dupes

        print(f"Number of Yellow duplicates removed: {yellow_removed_dupes}")
        logger.info(f"Number of Yellow duplicates removed: {yellow_removed_dupes}")

        remaining_dupes = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, COUNT(*) AS c
                FROM yellow_trips_clean
                GROUP BY 1,2,3,4,5
                HAVING c > 1
            )
        """).fetchone()[0]

        print(f"Number of Yellow duplicates remaining: {remaining_dupes}")
        logger.info(f"Number of Yellow duplicates remaining: {remaining_dupes}")
        '''

        # Filter out invalid yellow trips: passenger_count <= 0, distance <= 0 or >100, trips >24hr
        yellow_before = con.execute("SELECT COUNT(*) FROM yellow_trips").fetchone()[0]

        con.execute("""
            CREATE OR REPLACE TABLE yellow_trips_clean AS
            SELECT *
            FROM yellow_trips
            WHERE passenger_count > 0
              AND trip_distance > 0
              AND trip_distance <= 100
              AND DATEDIFF('hour', tpep_pickup_datetime, tpep_dropoff_datetime) <= 24;
        """)

        yellow_after = con.execute("SELECT COUNT(*) FROM yellow_trips_clean").fetchone()[0]
        yellow_removed = yellow_before - yellow_after

        print(f"Number of Yellow trips removed: {yellow_removed}")
        logger.info(f"Number of Yellow trips removed: {yellow_removed}")

        # Sanity checks for yellow trips
        bad_pass = con.execute("SELECT COUNT(*) FROM yellow_trips_clean WHERE passenger_count <= 0").fetchone()[0]
        bad_dist = con.execute("SELECT COUNT(*) FROM yellow_trips_clean WHERE trip_distance <= 0 OR trip_distance > 100").fetchone()[0]
        bad_dur = con.execute("SELECT COUNT(*) FROM yellow_trips_clean WHERE DATEDIFF('hour', tpep_pickup_datetime, tpep_dropoff_datetime) > 24").fetchone()[0]

        print(f"Number of Yellow bad passengers remaining: {bad_pass}")
        logger.info(f"Number of Yellow bad passengers remaining: {bad_pass}")
        print(f"Number of Yellow bad distance remaining: {bad_dist}")
        logger.info(f"Number of Yellow bad distance remaining: {bad_dist}")
        print(f"Number of Yellow bad duration remaining: {bad_dur}")
        logger.info(f"Number of Yellow bad duration remaining: {bad_dur}")

        # Remove duplicates from green_trips (active version, unlike yellow above)
        green_before_dupes = con.execute("SELECT COUNT(*) FROM green_trips").fetchone()[0]

        con.execute("""
            CREATE OR REPLACE TABLE green_trips_clean AS
            SELECT * EXCLUDE rn
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            vendorid,
                            lpep_pickup_datetime,
                            lpep_dropoff_datetime,
                            passenger_count,
                            trip_distance
                        ORDER BY lpep_pickup_datetime
                    ) AS rn
                FROM green_trips
            )
            WHERE rn = 1;
        """)

        green_after_dupes = con.execute("SELECT COUNT(*) FROM green_trips_clean").fetchone()[0]
        green_removed_dupes = green_before_dupes - green_after_dupes

        print(f"Number of Green duplicates removed: {green_removed_dupes}")
        logger.info(f"Number of Green duplicates removed: {green_removed_dupes}")

        # Sanity check: count any remaining duplicates in green_trips_clean
        remaining_dupes = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT vendorid, lpep_pickup_datetime, lpep_dropoff_datetime, passenger_count, trip_distance, COUNT(*) AS c
                FROM green_trips_clean
                GROUP BY 1,2,3,4,5
                HAVING c > 1
            )
        """).fetchone()[0]

        print(f"Number of Green duplicates remaining: {remaining_dupes}")
        logger.info(f"Number of Green duplicates remaining: {remaining_dupes}")

        # Filter out invalid green trips: passenger_count <= 0, distance <= 0 or >100, trips >24hr
        green_before = con.execute("SELECT COUNT(*) FROM green_trips_clean").fetchone()[0]

        con.execute("""
            CREATE OR REPLACE TABLE green_trips_clean AS
            SELECT *
            FROM green_trips_clean
            WHERE passenger_count > 0
              AND trip_distance > 0
              AND trip_distance <= 100
              AND DATEDIFF('hour', lpep_pickup_datetime, lpep_dropoff_datetime) <= 24;
        """)

        green_after = con.execute("SELECT COUNT(*) FROM green_trips_clean").fetchone()[0]
        green_removed = green_before - green_after

        print(f"Number of Green trips removed: {green_removed}")
        logger.info(f"Number of Green trips removed: {green_removed}")

        # Sanity checks for green trips
        bad_pass = con.execute("SELECT COUNT(*) FROM green_trips_clean WHERE passenger_count <= 0").fetchone()[0]
        bad_dist = con.execute("SELECT COUNT(*) FROM green_trips_clean WHERE trip_distance <= 0 OR trip_distance > 100").fetchone()[0]
        bad_dur = con.execute("SELECT COUNT(*) FROM green_trips_clean WHERE DATEDIFF('hour', lpep_pickup_datetime, lpep_dropoff_datetime) > 24").fetchone()[0]

        print(f"Number of Green bad passengers remaining: {bad_pass}")
        logger.info(f"Number of Green bad passengers remaining: {bad_pass}")
        print(f"Number of Green bad distance remaining: {bad_dist}")
        logger.info(f"Number of Green bad distance remaining: {bad_dist}")
        print(f"Number of Green bad duration remaining: {bad_dur}")
        logger.info(f"Number of Green bad duration remaining: {bad_dur}")

        # Final counts after cleaning process
        yellow_total_clean = con.execute("SELECT COUNT(*) FROM yellow_trips_clean").fetchone()[0]
        green_total_clean = con.execute("SELECT COUNT(*) FROM green_trips_clean").fetchone()[0]

        print(f"Total Yellow trips after cleaning: {yellow_total_clean}")
        print(f"Total Green trips after cleaning: {green_total_clean}")

        logger.info(f"Total Yellow trips after cleaning: {yellow_total_clean}")
        logger.info(f"Total Green trips after cleaning: {green_total_clean}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    clean_trips()
    print("Data cleaning process completed")
    logger.info("Data cleaning process completed")
