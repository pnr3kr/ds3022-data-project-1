

SELECT
    gt.*,
    
    -- CO2 per trip in kilograms
    gt.trip_distance * e.co2_grams_per_mile / 1000 AS trip_co2_kgs,
    
    -- Average MPH per trip
    CASE 
        WHEN DATEDIFF('second', gt.lpep_pickup_datetime, gt.lpep_dropoff_datetime) > 0
        THEN gt.trip_distance / (DATEDIFF('second', gt.lpep_pickup_datetime, gt.lpep_dropoff_datetime) / 3600.0)
        ELSE 0
    END AS avg_mph,
    
    -- Time breakdowns
    EXTRACT('hour' FROM gt.lpep_pickup_datetime)  AS hour_of_day,
    EXTRACT('dow'  FROM gt.lpep_pickup_datetime)  AS day_of_week,
    EXTRACT('week' FROM gt.lpep_pickup_datetime)  AS week_of_year,
    EXTRACT('month' FROM gt.lpep_pickup_datetime) AS month_of_year

FROM "emissions"."main"."green_trips_clean" gt
JOIN "emissions"."main"."emissions" e
  ON e.vehicle_type = 'green_taxi'