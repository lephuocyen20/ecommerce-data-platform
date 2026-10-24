WITH date_spine AS (

    SELECT
        calendar_date::date AS full_date

    FROM generate_series(
        DATE '2025-01-01',
        DATE '2030-12-31',
        INTERVAL '1 day'
    ) AS dates(calendar_date)

),

date_attributes AS (

    SELECT
        TO_CHAR(full_date, 'YYYYMMDD')::INTEGER AS date_key,

        full_date,

        EXTRACT(DAY FROM full_date)::INTEGER AS day_of_month,

        EXTRACT(ISODOW FROM full_date)::INTEGER AS day_of_week,

        TRIM(TO_CHAR(full_date, 'Day')) AS day_name,

        EXTRACT(WEEK FROM full_date)::INTEGER AS week_of_year,

        EXTRACT(MONTH FROM full_date)::INTEGER AS month_number,

        TRIM(TO_CHAR(full_date, 'Month')) AS month_name,

        EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter_number,

        EXTRACT(YEAR FROM full_date)::INTEGER AS year_number,

        (
            EXTRACT(ISODOW FROM full_date) IN (6, 7)
        ) AS is_weekend

    FROM date_spine

)

SELECT *
FROM date_attributes