{{ config(materialized='table') }}

SELECT
    yt.*,
    
    -- CO2 per trip in kilograms
    yt.trip_distance * e.co2_grams_per_mile / 1000 AS trip_co2_kgs,
    
    -- Average MPH per trip
    CASE 
        WHEN DATEDIFF('second', yt.tpep_pickup_datetime, yt.tpep_dropoff_datetime) > 0
        THEN yt.trip_distance / (DATEDIFF('second', yt.tpep_pickup_datetime, yt.tpep_dropoff_datetime) / 3600.0)
        ELSE 0
    END AS avg_mph,
    
    -- Time breakdowns
    EXTRACT('hour' FROM yt.tpep_pickup_datetime)  AS hour_of_day,
    EXTRACT('dow'  FROM yt.tpep_pickup_datetime)  AS day_of_week,
    EXTRACT('week' FROM yt.tpep_pickup_datetime)  AS week_of_year,
    EXTRACT('month' FROM yt.tpep_pickup_datetime) AS month_of_year

FROM {{ source('main', 'yellow_trips_clean') }} yt
JOIN {{ source('main', 'emissions') }} e
  ON e.vehicle_type = 'yellow_taxi'
