WITH source_data AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'payments') }}

),

renamed AS (

    SELECT
        payment_id,
        order_id,
        amount,
        payment_method,
        status,
        transaction_id,
        created_at,

        _silver_batch_id,
        _loaded_at

    FROM source_data

)

SELECT *
FROM renamed