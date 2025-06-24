CREATE OR REPLACE TABLE heatmap AS
        SELECT
        json_record:properties.place::string AS place,
        TRIM(SPLIT_PART(json_record:properties.place::string, ',', -1)) AS country,
        json_record:properties.mag::float AS magnitude,
        json_record:geometry.coordinates[0]::float AS longitude,
        json_record:geometry.coordinates[1]::float AS latitude,
        json_record:geometry.coordinates[2]::float AS depth,
        TO_TIMESTAMP_LTZ(json_record:properties.time::number / 1000) AS event_time
        FROM raw_data
        where trim(split_part(json_record:properties.place::string, ',', -1)) is not null;