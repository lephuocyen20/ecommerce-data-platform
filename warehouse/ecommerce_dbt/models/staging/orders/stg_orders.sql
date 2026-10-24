WITH source_data AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'orders') }}

),

renamed AS (

    SELECT
        order_id,
        customer_id,
        status,
        order_date,
        updated_at,

        _silver_batch_id,
        _loaded_at

    FROM source_data

)

SELECT *
FROM renamed