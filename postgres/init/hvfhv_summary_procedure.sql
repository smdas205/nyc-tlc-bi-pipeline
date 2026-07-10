/*
Procedure to fill "hvfhv_daily_summary" Aggregate Table
with data from "high_volume_fhv_trips"
for keeping less load on Power BI Report Generation
*/
CREATE OR REPLACE PROCEDURE hvfhv_summary_fill(start_date DATE, end_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    current_chunk DATE := start_date;
    next_chunk DATE;
BEGIN
    WHILE current_chunk < end_date LOOP
        next_chunk := current_chunk + INTERVAL '1 day';
        RAISE INFO 'Processing data for: %', current_chunk;
        RAISE NOTICE 'Processing data for: %', current_chunk;

        INSERT INTO hvfhv_daily_summary (
            pickup_date,
            total_trips,
            total_trip_distance,
            total_driver_pay,
            total_fare
        )
        SELECT
            (pickup_datetime AT TIME ZONE 'America/New_York')::date,
            COUNT(1),
            SUM(trip_distance),
            SUM(driver_pay),
            SUM(calculated_total_amount)
        FROM high_volume_fhv_trips
        WHERE pickup_datetime >= (current_chunk::timestamp AT TIME ZONE 'America/New_York')
          AND pickup_datetime <  (next_chunk::timestamp AT TIME ZONE 'America/New_York')
        GROUP BY
            (pickup_datetime AT TIME ZONE 'America/New_York')::date;

        COMMIT;
        current_chunk := next_chunk;
    END LOOP;
END;
$$;

SET log_min_messages = 'info';
