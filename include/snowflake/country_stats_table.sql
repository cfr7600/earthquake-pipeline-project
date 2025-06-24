CREATE OR REPLACE TABLE country_stats AS
        WITH extracted AS (
            SELECT
                country,
                place,
                magnitude,
                depth,
                event_time,
                longitude,
                latitude
            FROM heatmap
        ),
        ranked_events AS (
            SELECT
                country,
                magnitude,
                depth,
                event_time,
                LAG(event_time) OVER (PARTITION BY country ORDER BY event_time) AS prev_event_time
            FROM extracted
        ),
        intervals AS (
            SELECT
                country,
                magnitude,
                depth,
                event_time,
                DATEDIFF(day, prev_event_time, event_time) AS days_between
            FROM ranked_events
            WHERE prev_event_time IS NOT NULL
        ),
        largest_quakes AS (
            SELECT
                country,
                place AS largest_quake_place,
                event_time AS largest_quake_time,
                magnitude AS largest_quake_magnitude,
                depth AS largest_quake_depth,
                longitude AS largest_quake_longitude,
                latitude AS largest_quake_latitude,
                ROW_NUMBER() OVER (PARTITION BY country ORDER BY magnitude DESC, event_time DESC) AS rank
            FROM extracted
        )
        SELECT
            i.country,
            COUNT(*) AS quake_count,
            ROUND(AVG(i.magnitude), 3) AS avg_magnitude,
            ROUND(AVG(i.depth), 3) AS avg_depth,
            ROUND(AVG(i.days_between), 1) AS avg_days_between_quakes,
            l.largest_quake_magnitude,
            l.largest_quake_depth,
            l.largest_quake_place,
            l.largest_quake_time,
            l.largest_quake_longitude,
            l.largest_quake_latitude
        FROM intervals i
        JOIN largest_quakes l
            ON i.country = l.country AND l.rank = 1
        GROUP BY
            i.country,
            l.largest_quake_magnitude,
            l.largest_quake_depth,
            l.largest_quake_place,
            l.largest_quake_time,
            l.largest_quake_longitude,
            l.largest_quake_latitude
        HAVING COUNT(*) > 1
        ORDER BY quake_count DESC;