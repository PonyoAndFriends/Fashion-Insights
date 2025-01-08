WITH aggregated_data AS (
    SELECT
        column1,
        COUNT(column2) AS record_count,
        MAX(processed_date) AS latest_date
    FROM {{ ref('silver_table') }}
    GROUP BY column1
)
SELECT * FROM aggregated_data;
