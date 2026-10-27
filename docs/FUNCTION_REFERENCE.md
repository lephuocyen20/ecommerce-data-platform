# Tham chiếu hàm — E-Commerce Data Platform

Tài liệu này mô tả mã **do project sở hữu** (Python, DAG Airflow và dbt). Không
liệt kê hàm của thư viện bên thứ ba như Spark, psycopg2, MinIO hay dbt. Đường đi
chính là: `source PostgreSQL → Bronze/MinIO → Silver/Parquet → warehouse → dbt`.

## Quy ước đọc

- `conn` là kết nối PostgreSQL do caller quản lý transaction, trừ khi phần mô tả
  nói rõ hàm tự mở/đóng kết nối.
- `client` là MinIO client; `df`/`spark` là Spark DataFrame/SparkSession.
- Hàm có tên `main()` là entrypoint CLI/Docker, không phải API nghiệp vụ.
- Bất biến quan trọng: trạng thái batch đi theo `PENDING → UPLOADED → COMMITTED`.
  Watermark chỉ được tiến khi Bronze đã được xác minh.

## Bản đồ gọi hàm

```text
generator v2 → PostgreSQL source
  → run_incremental_ingestion_write / Airflow ingest_incremental
  → prepare_pending_batch → extract_incremental → stage_batch_csv
  → execute_pending_recovery → upload_recovered_batch → commit_uploaded_batch
  → transform_batch → Silver Parquet + completion manifest
  → process_batches → load_one_batch → upsert_raw_batch + insert_loaded_batch
  → dbt staging → dimensions/facts
```

## 1. Tạo dữ liệu nguồn

### `source/generator/v2/` (generator chính, có ràng buộc nghiệp vụ)

- `config.load_config()` đọc cấu hình generator; `config.get_db_connection()` mở
  kết nối DB.
- `cli.parse_args()` đọc tham số CLI; `runner.main()` điều phối toàn bộ v2:
  mở kết nối, sinh customer/product/order/payment/shipment, chèn dữ liệu và commit/rollback.
- `time_utils.random_timestamp(start, end)` chọn thời điểm ngẫu nhiên trong khoảng.
- `customers.generate_customers(cursor, n, start_date, end_date)` và
  `products.generate_products(cursor, n, start_date, end_date)` tạo entity cơ bản
  với thời điểm tạo hợp lệ.
- `orders.calculate_discount(quantity, unit_price)` tính chiết khấu; `generate_order_status(order_date, reference_date)` chọn trạng thái phù hợp tuổi đơn;
  `generate_order_updated_at(...)` tạo thời điểm cập nhật không sớm hơn lúc tạo.
  `generate_orders_with_items(cursor, n, end_date)` tạo order và item trong cùng
  logic để giữ FK/tổng tiền nhất quán.
- `payments.generate_payment_method()` chọn phương thức; `generate_payment_statuses(order_status, payment_method)` sinh chuỗi trạng thái hợp lệ.
  `generate_online_payment_timeline`, `generate_cod_payment_timeline`,
  `generate_refund_payment_timeline` sinh timestamp theo từng luồng thanh toán.
  `_payment_times(...)` chọn helper timeline; `_validate_records(records, orders)`
  kiểm tra dữ liệu trước insert; `generate_payments(cursor)` tạo records và
  `insert_payments(cursor, records)` ghi chúng.
- `shipments.generate_shipment_status(order_status)`, `generate_shipment_details(order_id)` và `generate_shipment_timeline(...)` tạo trạng thái, mã vận đơn và
  mốc giao hàng. `validate_shipments(records, orders, paid_order_ids)` áp ràng buộc
  nghiệp vụ (chỉ các order hợp lệ/đã thanh toán); `generate_shipments(cursor)` sinh
  bản ghi, `insert_shipments(cursor, records)` ghi DB.

## 2. Ingestion batch cũ

File `ingestion/postgres_to_minio.py` là luồng full/batch cũ.

- `validate_table_name(table_name)` chặn bảng ngoài schema cho phép.
- `get_minio_client()` tạo MinIO client.
- `extract_table(table_name)` đọc toàn bộ bảng PostgreSQL và trả columns/records.
- `records_to_csv(columns, records)` serialize dữ liệu thành CSV trong bộ nhớ.
- `upload_to_minio(client, table_name, csv_content)` ghi object Bronze và trả key/thông tin upload.
- `ingest_table(table_name)` ghép ba bước extract → CSV → upload.

## 3. Incremental ingestion

### Cấu hình và kết nối

- `config.get_table_config(table_name)` trả cấu hình bảng được hỗ trợ: primary key,
  watermark column/type; lỗi với bảng lạ.
- `config.validate_positive_integer(value, name)` bắt kiểu `int > 0`, đặc biệt loại
  `bool` dù Python xem bool là int. `validate_batch_id(batch_id)` kiểm tra UUID hex
  32 ký tự.
- `database.get_db_connection()` mở PostgreSQL từ environment. `require_transaction(conn)` bắt `autocommit=False`, bảo đảm hàm con không vô tình ghi ngoài transaction.

### Đọc tăng dần và staging

- `extractor.extract_incremental(conn, table_name, watermark, batch_size=1000)`
  truy vấn tối đa `batch_size` records sau checkpoint. Với watermark timestamp, so
  sánh cặp `(watermark_column, primary_key)` để không mất record cùng timestamp;
  với ID thì so sánh ID. Trả `{columns, rows, row_count, table_name}`.
- `staging.records_to_csv(columns, rows)` biến record thành CSV. `generate_batch_id()`
  tạo ID batch ngẫu nhiên.
- `prepare_batch_metadata(table_name, watermark, batch, csv_content)` tạo metadata:
  checkpoint đầu/cuối, checksum SHA-256, row count, object key và status `PENDING`.
- `stage_batch_csv(metadata, csv_content, staging_root='staging')` ghi tạm rồi publish
  theo kiểu không ghi đè; nếu file cùng batch đã có thì chỉ chấp nhận checksum giống.
- `load_staged_batch(metadata, staging_root='staging')` đọc lại file, xác minh hash
  và cấu trúc CSV trước recovery. `verify_existing()` là helper nội bộ của
  `stage_batch_csv` để kiểm tra file đích hiện hữu.

### MinIO và repository batch

- `minio_storage.build_object_key(table_name, batch_id)` tạo key Bronze chuẩn.
  `get_minio_client()` tạo client; `check_minio_bucket(client)` xác minh bucket.
- `object_exists(client, bucket_name, object_key)` kiểm tra object không làm thay đổi
  state. `verify_minio_object(...)` so SHA-256 object đã có với checksum mong đợi.
- `put_object_if_absent(...)` publish có precondition `If-None-Match`; chống ghi đè
  khi nhiều worker cùng recovery. `upload_incremental_batch(...)` là upload Bronze
  theo batch, mặc định `dry_run=True` để chỉ chuẩn bị/kiểm tra.
- `batch_repository.acquire_table_lock(conn, table_name)` lấy PostgreSQL advisory
  transaction lock theo table. `has_unfinished_batch`, `has_blocking_batch`,
  `has_failed_batch` truy vấn các status để quyết định có được bắt đầu batch mới.
- `register_pending_batch(conn, metadata)` insert metadata PENDING, không commit.
  `get_batch_metadata(conn, batch_id)` đọc một batch (hoặc `None`);
  `get_recoverable_batches(conn, table_name=None)` trả PENDING/UPLOADED theo thứ tự recovery.
  `lock_batch_metadata(conn, batch_id)` lock row bằng DB trước khi chuyển trạng thái.
- `mark_batch_uploaded(conn, client, batch_id)` xác minh object MinIO rồi đổi
  `PENDING → UPLOADED`, vẫn không commit.

### Watermark và recovery

- `watermark.get_watermark(conn, table_name)` đọc checkpoint hiện tại hoặc khởi tạo
  checkpoint trống theo loại watermark. `watermark_matches_batch_start(watermark, metadata)`
  ngăn commit một batch nếu checkpoint hiện tại không bằng checkpoint lúc batch bắt đầu.
- `get_locked_watermark(conn, table_name)` lấy advisory lock và đọc lại watermark.
  `commit_uploaded_batch(conn, client, batch_id)` atomically kiểm tra batch/object,
  checkpoint-start rồi cập nhật watermark và `UPLOADED → COMMITTED`.
- `recovery.validate_recovery_metadata(metadata)` kiểm tra shape, status, ID, hash,
  object key và checkpoint mà không chạm DB/MinIO.
- `recover_staged_batch(metadata, staging_root='staging')` gọi kiểm tra metadata,
  nạp CSV staged, hash và số record. `upload_recovered_batch(client, metadata, ...)`
  upload không ghi đè hoặc chấp nhận object concurrent nếu checksum khớp.
- `recover_uploaded_batch(conn, client, batch_id)` commit checkpoint cho batch đã
  UPLOADED. `execute_uploaded_recovery(...)` là wrapper transaction-owned.
  `execute_pending_recovery(...)` đọc staged file → upload/verify → mark uploaded
  → commit checkpoint; caller quyết định commit/rollback.

### Orchestrator public

- `prepare_pending_batch(conn, table_name, batch_size=1000, staging_root='staging')`
  lấy lock, chặn batch lỗi, đọc watermark, extract, stage CSV và register PENDING.
  Trả `None` khi không có data; không commit.
- `select_ingestion_action(table_name, recoverable_batches, has_blocking)` là hàm
  thuần quyết định `NEW_BATCH`, `RECOVER_PENDING` hoặc `RECOVER_UPLOADED`; từ chối
  nhiều batch dang dở/batch blocking.
- `execute_new_batch(...)` bọc `prepare_pending_batch`, trả result chuẩn `NO_DATA`
  hoặc `PENDING`. `execute_ingestion_action(...)` chạy chính xác một recovery action.
- `execute_ingestion_action_locked(...)` lấy lock trước, đọc lại state sau lock,
  chọn action rồi tạo/recover batch trong transaction của caller.
- `persist_new_pending_batch(..., allow_commit=False)` helper an toàn: chỉ cho ghi
  DB `ecommerce_test`; mặc định rollback, chỉ commit PENDING khi opt-in.
- `_execute_two_transaction_ingestion(...)` là lõi write: transaction 1 recover
  batch cũ hoặc commit PENDING mới; transaction 2 lock lại, recover batch đó và
  commit watermark. Nó kiểm tra metadata/table/batch ID ở mỗi biên transaction.
- `run_incremental_ingestion(...)` entrypoint quan sát: `dry_run=True` chỉ báo action
  và watermark; `rollback_test=True` chạy rồi rollback; production write bị chặn.
- `run_incremental_ingestion_write(..., allow_write=False)` entrypoint ghi có guard:
  cần `allow_write=True`, `INGESTION_WRITE_ENABLED=true`, DB đúng `ecommerce_test`
  và `autocommit=False`, sau đó gọi workflow hai transaction.
- `postgres_to_minio_incremental.main()` parse CLI và gọi các entrypoint trên.

## 4. Bronze → Silver bằng Spark

### Transform theo bảng

Các hàm `transform_customers(df)`, `transform_products(df)`,
`transform_orders(df)`, `transform_order_items(df)`, `transform_payments(df)`, và
`transform_shipments(df)` trong `transformation/transforms/` chuẩn hóa kiểu cột,
lọc bản ghi thiếu/không hợp lệ và loại duplicate theo khóa của từng entity. Chúng
nhận một Spark DataFrame và trả Spark DataFrame, không tự ghi storage.

### Luồng batch và incremental

- `bronze_to_silver.get_latest_bronze_path(client, bucket_name, table)` chọn object
  Bronze mới nhất; `configure_s3(spark)` cấu hình MinIO endpoint cho Spark;
  `read_bronze(spark, bronze_path)` đọc CSV; `load_manifest(...)` đọc manifest;
  `write_silver(df, silver_path)` ghi Parquet; `process_table(...)` điều phối cho
  một bảng. `main()` chạy biến thể batch cũ.
- `incremental_bronze_to_silver.validate_arguments(table_name, batch_id, object_key,
  bronze_checksum)` chặn input không hợp lệ. `get_minio_client()` tạo client;
  `verify_bronze_checksum(...)` xác minh object trước khi Spark đọc.
- `read_json_object(...)` đọc JSON từ MinIO; `get_completion_manifest(...)` lấy
  manifest Silver. `verify_published_manifest(...)` xác minh manifest hiện hữu
  khớp hoàn toàn với batch/object/checksum trước khi coi batch đã xong (idempotency).
- `publish_manifest(...)` publish completion manifest chỉ khi key chưa tồn tại;
  nếu concurrent write thì caller phải xác minh nội dung thay vì ghi đè.
- `transform_batch(spark, client, table_name, batch_id, object_key, bronze_checksum)`
  là entrypoint chính: validate → verify Bronze → read/transform → write Silver
  Parquet → verify files → publish manifest; trả metadata kết quả. `main()` là CLI.

### Silver snapshot

- `batch_discovery.get_committed_batches(conn, table_name)` lấy batch ingestion đã
  COMMITTED để build snapshot.
- `continuity.validate_batch_continuity(batches)` xác nhận end checkpoint batch trước
  khớp start checkpoint batch sau và watermark tăng dần.
- `silver_reader.read_verified_silver_batch(spark, client, bucket, batch, batch_sequence)`
  xác minh manifest/Parquet rồi đọc batch; thêm `_batch_sequence` để phân giải phiên bản.
- `assembler.assemble_silver_batches(...)` kiểm tra ID unique, thứ tự watermark,
  continuity, cùng table/schema rồi union các DataFrame.
- `merge.merge_silver_batches(df, primary_key, watermark_column)` từ chối key/null
  và duplicate version mơ hồ; window theo PK, watermark giảm dần, batch sequence giảm
  dần để giữ đúng record mới nhất.
- `builder.build_silver_snapshot(...)` compose assemble + merge và chỉ trả snapshot,
  không publish.

## 5. Kiểm tra chất lượng

- `validate_bronze.get_latest_object(...)` tìm Bronze object; `validate_table(table_name,
  object_name=None)` kiểm schema/content Bronze và báo pass/fail.
- `validate_incremental_bronze.validate_incremental_batch(table_name, batch_id)` xác
  minh một incremental batch Bronze cụ thể.
- `validate_silver.load_contract(path)` nạp YAML contract. `validate_primary_key`,
  `validate_foreign_key`, `validate_not_null`, `validate_non_negative`,
  `validate_positive`, `validate_unique` là các validator Spark, trả/raise theo lỗi
  dữ liệu. `run_check(description, validator, *args)` chuẩn hóa cách chạy/ghi nhận;
  `print_summary(...)` in tổng kết, `main()` áp contract cho toàn bộ Silver.
- Bản tối ưu `validate_silver_optimized.py`: `configured_columns(rules)` chọn cột
  cần đọc; `schema_error(df, expected_schema)` phát hiện lệch schema;
  `collect_table_metrics(df, rules)` gom nhiều aggregate trong một Spark pass;
  `add(aggregation, alias)` helper tạo aggregate; `validate_table_rules(...)` diễn
  giải metric theo contract; `missing(columns)` helper báo cột thiếu;
  `record_check(description, error=None)` ghi trạng thái và `main()` điều phối.

## 6. Warehouse loader

- `incremental_silver_to_warehouse.get_connection()` mở DB warehouse.
  `get_committed_batches(conn, selected_tables=None, selected_batch_ids=None)` tìm
  batch COMMITTED chưa load, có lọc selection; `get_minio_client()` tạo client.
- `check_silver_manifest(client, bucket, batch)`, `read_silver_manifest(...)` và
  `verify_parquet_objects(client, bucket, manifest)` xác minh manifest Silver và
  tất cả Parquet trước khi ghi warehouse. `main()` là CLI kiểm tra/discovery.
- `warehouse_prepare.prepare_raw_table(table_name, arrow_table)` tạo/điều chỉnh raw
  table đích theo Arrow schema.
- `raw_upsert.upsert_raw_batch(conn, table_name, batch_id, arrow_table)` upsert dữ
  liệu Silver vào raw warehouse, gắn batch ID và để transaction caller kiểm soát.
- `warehouse_checkpoint.insert_loaded_batch(conn, table_name, batch_id, row_count)`
  ghi checkpoint load sau upsert thành công.
- `warehouse_guards.acquire_table_lock(conn, table_name)` khóa table-level; 
  `check_batch_loadable(conn, batch)` từ chối batch đã load/không hợp lệ.
- `verify_silver_data.read_parquet(...)` đọc object; `load_verified_silver_batch(...)`
  nạp Arrow table sau đầy đủ verify; `verify_batch(...)` kiểm một batch; `main()` CLI.
- `warehouse_batch_loader.load_one_batch(conn, client, bucket, batch)` là đơn vị
  atomic: lock → guard → verify/read Silver → prepare → upsert → checkpoint; trả
  `LOADED` hoặc `SKIPPED`, không tự commit.
- `warehouse_orchestrator.process_batches(dry_run=True, selected_tables=None,
  selected_batch_ids=None, strict_batch_selection=False)` lặp các batch. Dry-run
  luôn rollback; commit thường theo batch để recovery, còn strict selection commit
  tất cả batch đã chọn như một transaction. `main()` parse `--dry-run|--commit`.

## 7. Airflow DAG

`ecommerce_pipeline.py` là DAG batch cũ: `ecommerce_pipeline()` khai báo DAG;
`ingest(table_name)` gọi ingest legacy; `create_batch_manifest(bronze_objects)` tạo
manifest; `validate(table_name, object_name)` gọi validator.

`ecommerce_incremental_ingestion.py` là DAG manual, có guard write:

- `ecommerce_incremental_ingestion()` tạo graph task. `validate_config()` kiểm tra
  mode, table/batch selection và environment trước các task.
- `ingest_incremental(table_name, config)` gọi ingestion và serialize result;
  `serialize(value)` đổi kiểu datetime/Path cho XCom. `validate_incremental(...)`
  xác minh Bronze output.
- `check_silver_eligibility(...)` branch sang transform hoặc `skip_silver(...)`.
  `verify_silver_manifest(...)` kiểm kết quả transform; `silver_result(...)` chuẩn
  hóa thành công/skip cho mỗi table.
- `pipeline_summary()` gộp kết quả mọi table. `warehouse_gate(summary)` quyết định
  có được ghi warehouse. `load_warehouse(gate_result, config)` gọi loader với các
  batch được gate cho phép.
- `check_dbt_eligibility(warehouse_result)` branch dbt/skip; `skip_dbt()` tạo trạng
  thái skip. `create_silver_transform_operator(table_name)` dựng DockerOperator
  Spark; `xcom_value(field)` tạo Jinja/XCom template cho operator.

## 8. dbt SQL models

Các file `.sql` không định nghĩa Python function. Mỗi `stg_*.sql` chuẩn hóa một raw
warehouse table. `dim_customers`, `dim_products`, `dim_date` tạo dimensions;
`fact_orders`, `fact_order_items`, `fact_payments`, `fact_shipments` tạo facts.
Các file `.yml` cạnh model định nghĩa schema tests/constraints, còn
`dbt_project.yml` cấu hình dbt project.

## 9. Tests và script kiểm thử

`tests/` không phải API runtime: mỗi `test_*` là test case mô tả contract của hàm
production. Nhóm chính là extractor/watermark, staging, batch repository/checkpoint,
recovery/retry, transaction flow/write safety, snapshot, và warehouse rollback.
Các script `warehouse/loader/test_*.py` cùng `tests/warehouse/test_*.py` có helper
`get_connection`, `count_*`, `checkpoint_exists`, `get_order` và `main`; chúng tạo
failure có chủ đích để chứng minh rollback không để dữ liệu/checkpoint dở dang.

Khi sửa một hàm production, nên chạy nhóm test cùng tên trước; ví dụ sửa
`commit_uploaded_batch` thì chạy `tests/test_checkpoint_commit.py` và
`tests/test_watermark.py`, sửa snapshot thì chạy `tests/test_snapshot_*.py`.
