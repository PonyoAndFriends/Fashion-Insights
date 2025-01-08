WITH cleaned_data AS (
    SELECT
        column1,
        column2,
        column3,
        CAST(column4 AS DATE) AS processed_date
    FROM {{ ref('staging_table') }}
)
SELECT * FROM cleaned_data;
