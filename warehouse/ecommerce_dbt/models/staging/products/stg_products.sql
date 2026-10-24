WITH source_data AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'products') }}

),

renamed AS (

    SELECT
        product_id,
        product_name,
        stock_quantity,
        price,
        created_at,
        updated_at,

        _silver_batch_id,
        _loaded_at

    FROM source_data

)

SELECT *
FROM renamed