WITH payments AS (

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

    FROM {{ ref('stg_payments') }}

),

orders AS (

    SELECT
        order_id,
        customer_id,
        order_date

    FROM {{ ref('stg_orders') }}

),

final AS (

    SELECT
        p.payment_id,
        p.order_id,

        o.customer_id,

        TO_CHAR(
            o.order_date,
            'YYYYMMDD'
        )::INTEGER AS order_date_key,

        TO_CHAR(
            p.created_at,
            'YYYYMMDD'
        )::INTEGER AS payment_date_key,

        p.created_at AS payment_created_at,

        p.payment_method,
        p.status AS payment_status,
        p.transaction_id,

        p.amount AS payment_amount,

        (p.status = 'SUCCESS') AS is_successful,

        p._silver_batch_id,
        p._loaded_at

    FROM payments p

    INNER JOIN orders o
        ON p.order_id = o.order_id

)

SELECT *
FROM final