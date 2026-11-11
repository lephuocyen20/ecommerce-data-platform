-- ============================================================================
-- E-Commerce Data Platform: schema khởi tạo PostgreSQL
-- Database đích: ecommerce_platform
--
-- Tạo toàn bộ state PostgreSQL cho môi trường mới:
--   1. Source transactional tables trong public
--   2. Metadata batch/watermark của incremental ingestion
--   3. Raw Warehouse tables và checkpoint của Warehouse load
--   4. Runtime metadata cho native Gold WRITE
--
-- File này không tạo dbt staging/mart objects. `dbt build` sẽ tạo chúng sau
-- khi Raw Warehouse load hoàn tất.
-- ============================================================================

BEGIN;

DO $$
BEGIN
    IF current_database() <> 'ecommerce_platform' THEN
        RAISE EXCEPTION
            'Unsafe database: %, expected ecommerce_platform',
            current_database();
    END IF;
END;
$$;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ============================================================================
-- 1. Transactional source tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id BIGSERIAL PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.products (
    product_id BIGSERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0
        CHECK (stock_quantity >= 0),
    price DECIMAL(12, 2) NOT NULL
        CHECK (price >= 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL
        REFERENCES public.customers(customer_id),
    status VARCHAR(30) NOT NULL
        CHECK (
            status IN (
                'PENDING', 'CONFIRMED', 'PROCESSING',
                'SHIPPED', 'DELIVERED', 'CANCELLED'
            )
        ),
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL
        REFERENCES public.orders(order_id),
    product_id BIGINT NOT NULL
        REFERENCES public.products(product_id),
    quantity INTEGER NOT NULL
        CHECK (quantity > 0),
    unit_price DECIMAL(12, 2) NOT NULL
        CHECK (unit_price >= 0),
    discount_amount DECIMAL(12, 2) NOT NULL DEFAULT 0
        CHECK (discount_amount >= 0),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMP,
    CHECK ((_is_deleted AND deleted_at IS NOT NULL) OR (NOT _is_deleted AND deleted_at IS NULL))
);

CREATE TABLE IF NOT EXISTS public.payments (
    payment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL
        REFERENCES public.orders(order_id),
    amount DECIMAL(12, 2) NOT NULL
        CHECK (amount > 0),
    payment_method VARCHAR(50) NOT NULL
        CHECK (
            payment_method IN (
                'BANK_TRANSFER', 'CREDIT_CARD', 'E_WALLET', 'COD',
                'CASH', 'MOMO'
            )
        ),
    status VARCHAR(30) NOT NULL
        CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED', 'REFUNDED')),
    transaction_id VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMP,
    CHECK ((_is_deleted AND deleted_at IS NOT NULL) OR (NOT _is_deleted AND deleted_at IS NULL))
);

CREATE TABLE IF NOT EXISTS public.shipments (
    shipment_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL
        REFERENCES public.orders(order_id),
    carrier VARCHAR(100) NOT NULL
        CHECK (carrier IN ('GHN', 'GHTK', 'Viettel Post', 'J&T Express')),
    tracking_number VARCHAR(100) NOT NULL UNIQUE,
    shipping_address TEXT NOT NULL,
    shipping_fee DECIMAL(12, 2) NOT NULL DEFAULT 0
        CHECK (shipping_fee >= 0),
    status VARCHAR(30) NOT NULL
        CHECK (
            status IN (
                'PENDING', 'PICKED_UP', 'IN_TRANSIT',
                'OUT_FOR_DELIVERY', 'DELIVERED', 'FAILED', 'RETURNED'
            )
        ),
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    estimated_delivery_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (delivered_at IS NULL OR shipped_at IS NOT NULL),
    CHECK (delivered_at IS NULL OR delivered_at >= shipped_at),
    CHECK (
        shipped_at IS NULL
        OR shipped_at >= created_at
    )
);

-- Index hỗ trợ join foreign key và extract theo timestamp watermark.
CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON public.orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON public.order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON public.order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_payments_order_id
    ON public.payments (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at_order_item_id
    ON public.order_items (updated_at, order_item_id);
CREATE INDEX IF NOT EXISTS idx_payments_updated_at_payment_id
    ON public.payments (updated_at, payment_id);
CREATE INDEX IF NOT EXISTS idx_shipments_order_id
    ON public.shipments (order_id);
CREATE INDEX IF NOT EXISTS idx_customers_updated_at_customer_id
    ON public.customers (updated_at, customer_id);
CREATE INDEX IF NOT EXISTS idx_products_updated_at_product_id
    ON public.products (updated_at, product_id);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at_order_id
    ON public.orders (updated_at, order_id);
CREATE INDEX IF NOT EXISTS idx_shipments_updated_at_shipment_id
    ON public.shipments (updated_at, shipment_id);

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_customers_updated_at ON public.customers;
CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE ON public.customers
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_products_updated_at ON public.products;
CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON public.products
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_orders_updated_at ON public.orders;
CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE ON public.orders
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_order_items_updated_at ON public.order_items;
CREATE TRIGGER trg_order_items_updated_at
BEFORE UPDATE ON public.order_items
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_payments_updated_at ON public.payments;
CREATE TRIGGER trg_payments_updated_at
BEFORE UPDATE ON public.payments
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

DROP TRIGGER IF EXISTS trg_shipments_updated_at ON public.shipments;
CREATE TRIGGER trg_shipments_updated_at
BEFORE UPDATE ON public.shipments
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

-- ============================================================================
-- 2. Control tables cho incremental ingestion
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.ingestion_watermarks (
    table_name VARCHAR(100) PRIMARY KEY
        CHECK (
            table_name IN (
                'customers', 'products', 'orders', 'order_items',
                'payments', 'shipments'
            )
        ),
    watermark_type VARCHAR(20) NOT NULL
        CHECK (watermark_type IN ('TIMESTAMP', 'ID')),
    last_timestamp TIMESTAMP,
    last_id BIGINT NOT NULL DEFAULT 0
        CHECK (last_id >= 0),
    last_batch_id VARCHAR(32),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    replay_cursor_timestamp TIMESTAMP,
    replay_cursor_id BIGINT NOT NULL DEFAULT 0 CHECK (replay_cursor_id >= 0),
    replay_upper_timestamp TIMESTAMP,
    replay_upper_id BIGINT NOT NULL DEFAULT 0 CHECK (replay_upper_id >= 0),
    CHECK (
        (watermark_type = 'TIMESTAMP')
        OR (watermark_type = 'ID' AND last_timestamp IS NULL)
    ),
    CHECK (
        last_batch_id IS NULL
        OR last_batch_id ~ '^[0-9a-f]{32}$'
    )
);

CREATE TABLE IF NOT EXISTS public.ingestion_batches (
    batch_id VARCHAR(32) PRIMARY KEY
        CHECK (batch_id ~ '^[0-9a-f]{32}$'),
    table_name VARCHAR(100) NOT NULL
        CHECK (
            table_name IN (
                'customers', 'products', 'orders', 'order_items',
                'payments', 'shipments'
            )
        ),
    watermark_type VARCHAR(20) NOT NULL
        CHECK (watermark_type IN ('TIMESTAMP', 'ID')),
    start_timestamp TIMESTAMP,
    start_id BIGINT NOT NULL DEFAULT 0
        CHECK (start_id >= 0),
    end_timestamp TIMESTAMP,
    end_id BIGINT NOT NULL
        CHECK (end_id > 0),
    row_count INTEGER NOT NULL
        CHECK (row_count > 0),
    object_key TEXT NOT NULL UNIQUE,
    checksum_sha256 VARCHAR(64) NOT NULL
        CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'UPLOADED', 'COMMITTED', 'FAILED')),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    replay_cursor_timestamp TIMESTAMP,
    replay_cursor_id BIGINT NOT NULL DEFAULT 0 CHECK (replay_cursor_id >= 0),
    replay_upper_timestamp TIMESTAMP,
    replay_upper_id BIGINT NOT NULL DEFAULT 0 CHECK (replay_upper_id >= 0),
    CHECK (
        (watermark_type = 'TIMESTAMP' AND end_timestamp IS NOT NULL)
        OR (
            watermark_type = 'ID'
            AND start_timestamp IS NULL
            AND end_timestamp IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_ingestion_batches_recovery
    ON public.ingestion_batches (table_name, status, created_at);
CREATE INDEX IF NOT EXISTS idx_ingestion_batches_committed_order
    ON public.ingestion_batches (
        table_name, end_timestamp, end_id, batch_id
    )
    WHERE status = 'COMMITTED';

-- ============================================================================
-- 3. Raw Warehouse và checkpoint load
-- Raw tables chủ động không có source foreign key: các Silver batch đã verify
-- có thể retry/load độc lập trước những batch liên quan.
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id BIGINT PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    _silver_batch_id VARCHAR(32) NOT NULL
        CHECK (_silver_batch_id ~ '^[0-9a-f]{32}$'),
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    stock_quantity INTEGER NOT NULL,
    price DECIMAL(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    _silver_batch_id VARCHAR(32) NOT NULL
        CHECK (_silver_batch_id ~ '^[0-9a-f]{32}$'),
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status VARCHAR(30) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    _silver_batch_id VARCHAR(32) NOT NULL
        CHECK (_silver_batch_id ~ '^[0-9a-f]{32}$'),
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    discount_amount DECIMAL(12, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMP,
    _silver_batch_id VARCHAR(32) NOT NULL
        CHECK (_silver_batch_id ~ '^[0-9a-f]{32}$'),
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL,
    transaction_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    _is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at TIMESTAMP,
    _silver_batch_id VARCHAR(32) NOT NULL
        CHECK (_silver_batch_id ~ '^[0-9a-f]{32}$'),
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.shipments (
    shipment_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    carrier VARCHAR(100) NOT NULL,
    tracking_number VARCHAR(100) NOT NULL,
    shipping_address TEXT NOT NULL,
    shipping_fee DECIMAL(12, 2) NOT NULL,
    status VARCHAR(30) NOT NULL,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    estimated_delivery_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    _silver_batch_id VARCHAR(32) NOT NULL
        CHECK (_silver_batch_id ~ '^[0-9a-f]{32}$'),
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_orders_customer_id
    ON raw.orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_order_items_order_id
    ON raw.order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_raw_order_items_product_id
    ON raw.order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_raw_payments_order_id
    ON raw.payments (order_id);
CREATE INDEX IF NOT EXISTS idx_raw_shipments_order_id
    ON raw.shipments (order_id);

CREATE TABLE IF NOT EXISTS warehouse.loaded_batches (
    batch_id VARCHAR(32) PRIMARY KEY
        CHECK (batch_id ~ '^[0-9a-f]{32}$'),
    table_name VARCHAR(100) NOT NULL
        CHECK (
            table_name IN (
                'customers', 'products', 'orders', 'order_items',
                'payments', 'shipments'
            )
        ),
    watermark_type VARCHAR(20) NOT NULL
        CHECK (watermark_type IN ('TIMESTAMP', 'ID')),
    start_timestamp TIMESTAMP,
    start_id BIGINT NOT NULL
        CHECK (start_id >= 0),
    end_timestamp TIMESTAMP,
    end_id BIGINT NOT NULL
        CHECK (end_id > 0),
    silver_manifest_key TEXT NOT NULL UNIQUE,
    silver_data_prefix TEXT NOT NULL UNIQUE,
    silver_row_count INTEGER NOT NULL
        CHECK (silver_row_count >= 0),
    loaded_row_count INTEGER NOT NULL
        CHECK (loaded_row_count >= 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (watermark_type = 'TIMESTAMP' AND end_timestamp IS NOT NULL)
        OR (
            watermark_type = 'ID'
            AND start_timestamp IS NULL
            AND end_timestamp IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_loaded_batches_table_watermark
    ON warehouse.loaded_batches (
        table_name, end_timestamp DESC NULLS LAST, end_id DESC
    );

COMMIT;

-- ============================================================================
-- Query chỉ đọc để kiểm tra bootstrap thủ công
-- ============================================================================

SELECT current_database() AS database_name;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('public', 'raw', 'warehouse')
  AND table_name IN (
      'customers', 'products', 'orders', 'order_items', 'payments',
      'shipments', 'ingestion_watermarks', 'ingestion_batches',
      'loaded_batches'
  )
ORDER BY table_schema, table_name;
