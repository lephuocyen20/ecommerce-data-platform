WITH source_data AS (

    SELECT *
    FROM {{ source('ecommerce_raw', 'customers') }}

),

renamed AS (

    SELECT
        customer_id,
        full_name,
        email,
        created_at,
        updated_at,

        _silver_batch_id,
        _loaded_at

    FROM source_data

)

SELECT *
FROM renamed
