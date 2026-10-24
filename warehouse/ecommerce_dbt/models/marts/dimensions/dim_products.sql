WITH products AS (

    SELECT
        product_id,
        product_name,
        stock_quantity,
        price,
        created_at,
        updated_at,
        _silver_batch_id,
        _loaded_at

    FROM {{ ref('stg_products') }}

)

SELECT
    product_id,
    product_name,
    stock_quantity,
    price,
    created_at,
    updated_at,
    _silver_batch_id,
    _loaded_at

FROM products