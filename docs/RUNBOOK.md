# Hướng dẫn vận hành E-Commerce Data Platform

Tài liệu này hướng dẫn chạy local toàn bộ luồng hiện tại:

`PostgreSQL → Bronze (MinIO) → Silver (Spark) → Raw Warehouse (PostgreSQL) → dbt marts`.

Chạy lệnh từ thư mục gốc repository bằng PowerShell. Pipeline incremental mặc định là
`DRY_RUN`; chỉ `WRITE` mới ghi Bronze, Silver, Warehouse và chạy dbt.

## 1. Điều kiện và cấu hình

- Docker Desktop đang chạy; tài khoản chạy Docker phải truy cập được Docker socket.
- Python virtual environment chỉ cần khi chạy generator, batch pipeline hoặc test trên host.
- Tạo `.env` từ `.env.example`, điền secret thật và **không commit** file này.
- `DB_NAME` phải là `ecommerce_platform`. Đây là database được incremental ingestion,
  Warehouse và dbt sử dụng trong cấu hình hiện tại.
- Các biến `WAREHOUSE_DB_*` của Airflow phải trỏ tới cùng database với `DB_*`.

Các biến quan trọng:

```text
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_platform
DB_USER=ecommerce_user
DB_PASSWORD=<secret>

MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=<access-key>
MINIO_SECRET_KEY=<secret-key>
BRONZE_BUCKET=ecommerce-data-lake

INGESTION_WRITE_ENABLED=true
# Chỉ bật ngay trước một lượt WRITE đã được kiểm tra.
WAREHOUSE_WRITE_ENABLED=true
```

`WAREHOUSE_WRITE_ENABLED` của scheduler mặc định là `false` trong
`docker-compose.yml`; đây là chốt bảo vệ độc lập với `INGESTION_WRITE_ENABLED`.

> PostgreSQL chỉ chạy script khởi tạo trong `source/database/` khi named volume được
> tạo lần đầu. Khi dựng mới, phải có schema cho `ecommerce_platform`, bao gồm Raw Warehouse
> và `warehouse.loaded_batches`, trước khi chạy pipeline. Không xoá volume hiện có nếu
> chưa sao lưu dữ liệu cần giữ.

## 2. Build image và khởi động hạ tầng

Incremental DAG dùng DockerOperator để khởi tạo Spark và dbt containers, nên hai image
này phải tồn tại trước khi trigger DAG.

```powershell
# Build Spark image và chạy PostgreSQL, MinIO, Spark
docker compose -f docker-compose.yml -f spark/docker-compose.spark.yml build spark
docker compose -f docker-compose.yml -f spark/docker-compose.spark.yml up -d postgres minio spark

# Build image dùng bởi task dbt_build của Airflow
docker compose --profile dbt build dbt-runner

# Chỉ cần ở lần setup đầu hoặc sau khi đổi metadata database
docker compose run --rm airflow-init

# Scheduler có Docker socket để chạy Spark/dbt DockerOperator
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
docker compose ps
```

MinIO Console: `http://localhost:9001`. Airflow: `http://localhost:8085`.

Đăng nhập Airflow bằng `AIRFLOW_ADMIN_USERNAME` và
`AIRFLOW_ADMIN_PASSWORD` trong `.env`. Đặt password riêng trước lần chạy đầu;
credential này được lưu trong Docker volume `airflow_auth` nên không bị đổi
khi recreate container.

Sau khi đổi `.env` hoặc `docker-compose.yml`, recreate các Airflow services:

```powershell
docker compose up -d --force-recreate airflow-api-server airflow-scheduler airflow-dag-processor
```

## 3. Tạo dữ liệu nguồn (tùy chọn)

Generator V2 có entry point `source/generator/generate_data_v2.py`; chạy theo thứ tự:

```powershell
# customers và products
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage master

# orders và order_items
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage orders

# Hai stage sau chỉ kiểm tra trong bộ nhớ nếu thiếu --insert
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage payments --insert
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage shipments --insert
```

Có thể thêm `--customers`, `--products`, `--orders`, `--months` và `--seed`. `master`
và `orders` từ chối chạy lại nếu bảng đích đã có dữ liệu; chỉ làm sạch database test khi
chủ đích tạo dataset mới.

## 4. Chạy pipeline incremental qua Airflow

Mở Airflow, chọn `ecommerce_incremental_ingestion` và bấm **Trigger DAG**. DAG chỉ chạy
thủ công (`schedule=None`) và hỗ trợ: `customers`, `products`, `orders`, `order_items`,
`payments`, `shipments`.

Luồng WRITE thành công:

```text
validate_config → incremental Bronze → Bronze validation → Spark Silver
→ Silver manifest verification → Warehouse gate → transactional Raw Warehouse load
→ dbt build
```

### Kiểm tra an toàn bằng DRY_RUN

`DRY_RUN` là mặc định và không ghi dữ liệu. Trigger với cấu hình rỗng để kiểm tra toàn
bộ sáu bảng:

```json
{}
```

Hoặc chỉ kiểm tra một số bảng:

```json
{
  "mode": "DRY_RUN",
  "tables": ["orders", "order_items"]
}
```

Warehouse trả `SKIPPED` với lý do `DRY_RUN` và nhánh dbt chạy `skip_dbt`. Kiểm tra log
`validate_config`, `validate_*`, `verify_silver_*`, `warehouse_gate` và `load_warehouse`
trước khi cho phép ghi.

### Chạy WRITE có kiểm soát

1. Hoàn thành `DRY_RUN` cho đúng các bảng cần xử lý.
2. Đặt `INGESTION_WRITE_ENABLED=true` và `WAREHOUSE_WRITE_ENABLED=true` cho scheduler,
   rồi recreate Airflow services bằng lệnh ở phần 2.
3. Trigger với danh sách bảng tường minh:

```json
{
  "mode": "WRITE",
  "tables": ["orders", "order_items"]
}
```

`WRITE` không có `tables`, có bảng không hỗ trợ hoặc bảng lặp sẽ thất bại tại
`validate_config`. Nếu `INGESTION_WRITE_ENABLED` không phải `true`, ingestion bị chặn.
Nếu warehouse guard chưa bật, Bronze/Silver có thể hoàn thành nhưng Warehouse trả
`SKIPPED` với `WAREHOUSE_WRITE_DISABLED` và dbt không chạy.

Khi warehouse gate chấp nhận Silver, loader chỉ xử lý các batch đã được DAG xác minh.
Loader upsert vào Raw Warehouse và ghi checkpoint vào `warehouse.loaded_batches` trong
cùng transaction; lỗi sẽ rollback. dbt chỉ chạy sau khi loader trả `COMPLETED` và
checkpoint được kiểm tra lại.

### Iceberg order_items cutover

Keep `ORDER_ITEMS_SILVER_SOURCE=parquet` during the dual run. Before enabling
Iceberg, run the read-only reconciliation command in `LAKEHOUSE.md` and require
matching rows, business hashes, quantity, gross amount, discount, net amount,
and order/product foreign keys. For one reviewed WRITE run only, set:

```text
ORDER_ITEMS_SILVER_SOURCE=iceberg
ORDER_ITEMS_ICEBERG_CUTOVER_ENABLED=true
```

Recreate the Airflow services after changing the environment. The DAG records
the pinned Iceberg snapshot in `warehouse.loaded_batches`; return the source to
`parquet` to roll back without deleting either copy.

### Gold snapshot WRITE

Gold chỉ publish snapshot khi `GOLD_SNAPSHOT_WRITE_ENABLED=true`. Với database đã được tạo
trước khi Gold WRITE được bổ sung, áp dụng hai migration fencing một lần trước khi trigger DAG:

```powershell
Get-Content source/database/migrations/001_create_gold_execution_fencing.sql |
  docker exec -i ecommerce-postgres psql -U ecommerce_user -d <database-name>
Get-Content source/database/migrations/002_create_gold_execution_attempt_history.sql |
  docker exec -i ecommerce-postgres psql -U ecommerce_user -d <database-name>
```

Khi flag được bật và Warehouse/dbt hoàn tất, mỗi Gold dataset sẽ kiểm tra source snapshot,
lấy advisory lock + fencing token, ghi Parquet bất biến, xác minh checksum/schema/primary key,
rồi mới publish `manifest.json`. Snapshot cùng lineage đã publish chỉ được xác minh lại và trả về
`ALREADY_PUBLISHED`; không bị ghi đè.

Sau WRITE, tắt lại `WAREHOUSE_WRITE_ENABLED` (và `INGESTION_WRITE_ENABLED` nếu không cần),
recreate Airflow services, rồi lưu run ID và log cho audit.

## 5. Kiểm tra Warehouse và dbt

Task `dbt_build` chạy `dbt build` trong image
`ecommerce-data-platform-dbt-runner:latest`. Nó tạo/kiểm tra staging models cùng các
marts: `dim_customers`, `dim_products`, `dim_date`, `fact_orders`, `fact_order_items`,
`fact_payments`, `fact_shipments`.

Chạy hoặc chẩn đoán dbt thủ công bằng service profile `dbt`:

```powershell
docker compose --profile dbt run --rm dbt-runner debug
docker compose --profile dbt run --rm dbt-runner build
```

Warehouse loader thủ công chỉ nên dùng `--dry-run` để kiểm tra. `--commit` có thể ghi
các batch pending ngoài phạm vi một DAG run, nên không thay thế workflow Airflow.

```powershell
.\.venv\Scripts\python.exe warehouse\loader\warehouse_orchestrator.py --dry-run
```

## 6. Chẩn đoán và dừng dịch vụ

```powershell
docker compose ps
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-dag-processor
docker compose logs -f postgres
docker compose logs -f minio
docker logs -f ecommerce-spark
```

Khi một lượt incremental thất bại, kiểm tra theo thứ tự:

1. `validate_config`: mode, table selection và `INGESTION_WRITE_ENABLED`.
2. `validate_<table>`/`verify_silver_<table>`: batch ID, checksum và manifest.
3. `warehouse_gate`/`load_warehouse`: warehouse guard, batch selection và checkpoint.
4. `dbt_build`: biến `DBT_*`, Raw Warehouse và kết quả model/test.

```powershell
docker compose down
```

Lệnh trên dừng và xoá containers nhưng giữ named volumes. Chỉ xoá volume sau khi sao lưu
hoặc khi chủ đích khởi tạo lại môi trường local.
