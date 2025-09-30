import duckdb
import logging
import matplotlib.pyplot as plt
from pathlib import Path
import calendar

# Configure logging for analysis
logging.basicConfig(
    filename="logs/analysis.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level="DEBUG"
)
logger = logging.getLogger(__name__)


def analyze_trips():
    con = None
    try:
        # Connect to DuckDB database
        con = duckdb.connect(database="emissions.duckdb", read_only=False)
        print("Connected to DuckDB for analysis")
        logger.info("Connected to DuckDB for analysis")

        yellow_tbl = "main.yellow_trips_transformed"
        green_tbl = "main.green_trips_transformed"

        # Maps numeric day/month values to human-readable labels
        dow_map = {i: name for i, name in enumerate(calendar.day_abbr)}  # 0=Sun, 6=Sat
        month_map = {i: name for i, name in enumerate(calendar.month_abbr) if i > 0}  # 1=Jan, 12=Dec

        # Find the single largest carbon-producing trip for each cab type (2015–2024)
        for name, tbl, pickup, dropoff in [
            ("YELLOW", yellow_tbl, "tpep_pickup_datetime", "tpep_dropoff_datetime"),
            ("GREEN", green_tbl, "lpep_pickup_datetime", "lpep_dropoff_datetime"),
        ]:
            row = con.execute(f"""
                SELECT trip_co2_kgs, trip_distance, {pickup}, {dropoff}
                FROM {tbl}
                WHERE {pickup} BETWEEN '2015-01-01' AND '2024-12-31'
                ORDER BY trip_co2_kgs DESC
                LIMIT 1
            """).fetchone()

            msg = (f"Largest {name} CO2 trip (2015–2024): "
                   f"{row[0]:.2f} kg, {row[1]:.2f} miles, {row[2]} --> {row[3]}")
            print(msg)
            logger.info(msg)

        # Helper: calculate most and least carbon-heavy buckets (hour, day, week, month)
        def report_for_table(table, bucket_sql, pickup):
            max_row = con.execute(f"""
                SELECT {bucket_sql} AS bucket, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE {pickup} BETWEEN '2015-01-01' AND '2024-12-31'
                GROUP BY bucket
                ORDER BY avg_co2 DESC
                LIMIT 1
            """).fetchone()

            min_row = con.execute(f"""
                SELECT {bucket_sql} AS bucket, AVG(trip_co2_kgs) AS avg_co2
                FROM {table}
                WHERE {pickup} BETWEEN '2015-01-01' AND '2024-12-31'
                GROUP BY bucket
                ORDER BY avg_co2 ASC
                LIMIT 1
            """).fetchone()

            return max_row, min_row

        # Report most/least carbon-heavy hours, days, weeks, and months
        for name, tbl, pickup in [
            ("YELLOW", yellow_tbl, "tpep_pickup_datetime"),
            ("GREEN", green_tbl, "lpep_pickup_datetime"),
        ]:
            # Hour of day (numeric)
            max_row, min_row = report_for_table(tbl, f"EXTRACT('hour' FROM {pickup})", pickup)
            print(f"{name} most carbon-heavy HOUR (2015–2024): {int(max_row[0])}, avg {max_row[1]:.2f} kg")
            print(f"{name} least carbon-heavy HOUR (2015–2024): {int(min_row[0])}, avg {min_row[1]:.2f} kg")

            # Day of week (map to names like Mon, Tue, etc.)
            max_row, min_row = report_for_table(tbl, f"EXTRACT('dow' FROM {pickup})", pickup)
            print(f"{name} most carbon-heavy DAY (2015–2024): {dow_map[int(max_row[0])]}, avg {max_row[1]:.2f} kg")
            print(f"{name} least carbon-heavy DAY (2015–2024): {dow_map[int(min_row[0])]}, avg {min_row[1]:.2f} kg")

            # Week of year (numeric only)
            max_row, min_row = report_for_table(tbl, f"EXTRACT('week' FROM {pickup})", pickup)
            print(f"{name} most carbon-heavy WEEK (2015–2024): Week {int(max_row[0])}, avg {max_row[1]:.2f} kg")
            print(f"{name} least carbon-heavy WEEK (2015–2024): Week {int(min_row[0])}, avg {min_row[1]:.2f} kg")

            # Month of year (map to names like Jan, Feb, etc.)
            max_row, min_row = report_for_table(tbl, f"EXTRACT('month' FROM {pickup})", pickup)
            print(f"{name} most carbon-heavy MONTH (2015–2024): {month_map[int(max_row[0])]}, avg {max_row[1]:.2f} kg")
            print(f"{name} least carbon-heavy MONTH (2015–2024): {month_map[int(min_row[0])]}, avg {min_row[1]:.2f} kg")

        # Calculate monthly totals across all 10 years (for plotting)
        monthly_yellow = con.execute(f"""
            SELECT strftime('%Y-%m', tpep_pickup_datetime) AS ym, SUM(trip_co2_kgs)
            FROM {yellow_tbl}
            WHERE tpep_pickup_datetime BETWEEN '2015-01-01' AND '2024-12-31'
            GROUP BY ym
            ORDER BY ym
        """).fetchall()

        monthly_green = con.execute(f"""
            SELECT strftime('%Y-%m', lpep_pickup_datetime) AS ym, SUM(trip_co2_kgs)
            FROM {green_tbl}
            WHERE lpep_pickup_datetime BETWEEN '2015-01-01' AND '2024-12-31'
            GROUP BY ym
            ORDER BY ym
        """).fetchall()

        # Convert DuckDB rows into x and y lists for plotting
        def to_series(rows):
            xs = [r[0] for r in rows]
            ys = [float(r[1]) for r in rows]
            return xs, ys

        x_y, y_y = to_series(monthly_yellow)
        x_g, y_g = to_series(monthly_green)

        # Create plots/ folder if it doesn’t exist
        Path("plots").mkdir(exist_ok=True)
        plt.figure(figsize=(12, 6))

        # Plot Yellow and Green monthly totals
        plt.plot(x_y, y_y, marker="o", markersize=3, label="Yellow", color="gold", linewidth=1)
        plt.plot(x_g, y_g, marker="o", markersize=3, label="Green", color="green", linewidth=1)

        # Title and axis labels
        plt.title("Monthly CO₂ Totals (kg) — 2015–2024")
        plt.xlabel("Month-Year")
        plt.ylabel("Total CO₂ (kg)")

        # Show only yearly ticks on x-axis (every 12 months)
        plt.xticks(ticks=range(0, len(x_y), 12), labels=[x_y[i] for i in range(0, len(x_y), 12)], rotation=45)

        # Cleaner grid (major only)
        plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.7, which="major")
        plt.minorticks_off()

        # Remove top/right borders
        plt.gca().spines["top"].set_visible(False)
        plt.gca().spines["right"].set_visible(False)

        # Finalize and save plot
        plt.legend()
        plt.tight_layout()
        out_path = "plots/decade_monthly_co2_totals.png"
        plt.savefig(out_path, dpi=300)
        plt.close()

        print(f"Saved plot: {out_path}")
        logger.info(f"Saved plot: {out_path}")

    except Exception as e:
        # Catch and log errors
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    analyze_trips()
    print("Analysis process completed")
    logger.info("Analysis process completed")
