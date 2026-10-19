CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    stock_quantity INT NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    price DECIMAL(12,2) NOT NULL CHECK (price >= 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    status VARCHAR(30) NOT NULL CHECK (
        status IN ('PENDING','CONFIRMED','PROCESSING','SHIPPED','DELIVERED','CANCELLED')
    ),
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    product_id BIGINT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(12,2) NOT NULL CHECK (unit_price >= 0),
    discount_amount DECIMAL(12,2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0)
);

CREATE TABLE payments (
    payment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    amount DECIMAL(12,2) NOT NULL CHECK (amount > 0),
    payment_method VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL CHECK (
        status IN ('PENDING','SUCCESS','FAILED','REFUNDED')
    ),
    transaction_id VARCHAR(100) UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE shipments (
    shipment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(order_id),
    carrier VARCHAR(100),
    tracking_number VARCHAR(100) UNIQUE,
    shipping_address TEXT NOT NULL,
    shipping_fee DECIMAL(12,2) NOT NULL DEFAULT 0 CHECK (shipping_fee >= 0),
    status VARCHAR(30) NOT NULL CHECK (
        status IN ('PENDING','PICKED_UP','IN_TRANSIT','OUT_FOR_DELIVERY','DELIVERED','FAILED','RETURNED')
    ),
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    estimated_delivery_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


# Tạo function dùng chung BEFORE_UPDATE
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON public.customers
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON public.products
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TRIGGER trg_shipments_updated_at
BEFORE UPDATE ON public.shipments
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();


#Lưu check_point đã hoàn thành cho từng bảng
CREATE TABLE IF NOT EXISTS public.ingestion_watermarks (
    table_name VARCHAR(100) PRIMARY KEY,
    watermark_type VARCHAR(20) NOT NULL
        CHECK (watermark_type IN ('TIMESTAMP', 'ID')),
    last_timestamp TIMESTAMP,
    last_id BIGINT NOT NULL DEFAULT 0,
    last_batch_id VARCHAR(100),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- E-COMMERCE DATA PLATFORM
-- WEEK 6 - INCREMENTAL INGESTION
-- TABLE: public.ingestion_batches
--
-- Purpose:
--   Track ingestion batches and support retry/recovery.
--
-- Target database: ecommerce_test
-- ============================================================

BEGIN;

-- 1. Safety check
DO $$
BEGIN
    IF current_database() <> 'ecommerce_test' THEN
        RAISE EXCEPTION
            'Unsafe database: %, expected ecommerce_test',
            current_database();
    END IF;
END;
$$;

-- 2. Create batch tracking table
CREATE TABLE public.ingestion_batches (

    -- Batch identification
    batch_id VARCHAR(32) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    watermark_type VARCHAR(20) NOT NULL,

    -- Starting watermark
    start_timestamp TIMESTAMP NULL,
    start_id BIGINT NOT NULL DEFAULT 0,

    -- Ending watermark
    end_timestamp TIMESTAMP NULL,
    end_id BIGINT NULL,

    -- Batch metadata
    row_count INTEGER NOT NULL,
    object_key TEXT NOT NULL UNIQUE,
    checksum_sha256 VARCHAR(64) NOT NULL,

    -- Processing status
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',

    -- Audit timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Allowed source tables
    CONSTRAINT ingestion_batches_table_check
        CHECK (
            table_name IN (
                'customers',
                'products',
                'orders',
                'order_items',
                'payments',
                'shipments'
            )
        ),

    -- Watermark types
    CONSTRAINT ingestion_batches_watermark_type_check
        CHECK (watermark_type IN ('TIMESTAMP', 'ID')),

    -- Batch statuses
    CONSTRAINT ingestion_batches_status_check
        CHECK (
            status IN (
                'PENDING',
                'UPLOADED',
                'COMMITTED',
                'FAILED'
            )
        ),

    -- Row count validation
    CONSTRAINT ingestion_batches_row_count_check
        CHECK (row_count > 0),

    -- Starting ID validation
    CONSTRAINT ingestion_batches_start_id_check
        CHECK (start_id >= 0),

    -- Ending ID validation
    CONSTRAINT ingestion_batches_end_id_check
        CHECK (end_id IS NOT NULL AND end_id > 0),

    -- Batch ID format: UUID hex (32 characters)
    CONSTRAINT ingestion_batches_batch_id_check
        CHECK (batch_id ~ '^[0-9a-f]{32}$'),

    -- SHA-256 checksum format
    CONSTRAINT ingestion_batches_checksum_check
        CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),

    -- Timestamp watermark validation
    CONSTRAINT ingestion_batches_timestamp_check
        CHECK (
            (
                watermark_type = 'TIMESTAMP'
                AND end_timestamp IS NOT NULL
            )
            OR
            (
                watermark_type = 'ID'
                AND start_timestamp IS NULL
                AND end_timestamp IS NULL
            )
        )
);

COMMIT;

-- ============================================================
-- VERIFICATION QUERIES (READ-ONLY)
-- ============================================================

-- Check table existence and number of batches
SELECT
    current_database() AS database_name,
    COUNT(*) AS total_batches
FROM public.ingestion_batches;

-- Check all constraints
SELECT
    conname AS constraint_name,
    contype AS constraint_type
FROM pg_constraint
WHERE conrelid = 'public.ingestion_batches'::regclass
ORDER BY conname;
