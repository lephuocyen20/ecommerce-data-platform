WITH customers AS (

    SELECT
        customer_id,
        full_name,
        email,
        created_at,
        updated_at,
        _silver_batch_id,
        _loaded_at

    FROM {{ ref('stg_customers') }}

)

SELECT
    customer_id,
    full_name,
    email,
    created_at,
    updated_at,
    _silver_batch_id,
    _loaded_at

FROM customers