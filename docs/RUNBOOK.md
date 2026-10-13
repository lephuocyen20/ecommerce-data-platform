# Hướng dẫn chạy Ecommerce Data Platform

Tài liệu này mô tả luồng batch chạy local: PostgreSQL nguồn -> MinIO Bronze ->
Parquet Silver -> kiểm tra chất lượng dữ liệu. Chạy toàn bộ lệnh trong thư mục
gốc của project bằng PowerShell.

## Điều kiện cần có

- Docker Desktop đang chạy.
- File `.env` có `DB_PASSWORD`, `MINIO_ACCESS_KEY`, và `MINIO_SECRET_KEY`.
- Khi chạy Python trên máy host, môi trường ảo cần có `psycopg2-binary`,
  `minio`, `python-dotenv`, `faker`, `pyspark`, và `PyYAML`.

Các lệnh chạy Spark container bên dưới không yêu cầu Java trên máy host. Nếu
chạy PySpark local thì cần cài Java và cấu hình `JAVA_HOME`.

## 1. Khởi động PostgreSQL, MinIO và Spark

```powershell
docker compose -f docker-compose.yml -f spark/docker-compose.spark.yml up -d postgres minio spark
docker compose ps
```

PostgreSQL khởi tạo schema ecommerce từ `source/database/schema.sql` ở lần đầu
tạo Docker volume. MinIO Console chạy tại `http://localhost:9001`.

## 2. Sinh dữ liệu nguồn trong PostgreSQL

Generator V2 dùng một entry point duy nhất: `source/generator/generate_data_v2.py`.
Logic được tách theo bảng trong `source/generator/v2/`, nhưng không cần chạy từng
module riêng lẻ.

> **Lưu ý về database:** V2 chỉ kết nối database `ecommerce_test`, theo cấu hình
> trong `source/generator/.env.test`. Docker Compose mặc định tạo database
> `ecommerce`, là database được ingestion và pipeline sử dụng. Vì vậy V2 là công cụ
> tạo/kiểm thử dữ liệu tách biệt; không tự động cung cấp dữ liệu cho pipeline mặc
> định. Cần có database `ecommerce_test` đã áp dụng schema trước khi chạy V2.

Tạo dữ liệu theo đúng thứ tự phụ thuộc dưới đây:

```powershell
# Tạo customers và products
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage master

# Tạo orders và order_items
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage orders

# Sinh và kiểm tra payment records trong bộ nhớ
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage payments
```

Lệnh tổng quát là:

```powershell
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage <master|orders|payments>
```

Có thể điều chỉnh quy mô và dữ liệu sinh ngẫu nhiên bằng các tham số `--customers`,
`--products`, `--orders`, `--months`, và `--seed`. Ví dụ:

```powershell
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage master --customers 1000 --products 300 --months 12 --seed 42
.\.venv\Scripts\python.exe source\generator\generate_data_v2.py --stage orders --orders 5000 --months 12 --seed 42
```

Stage `master` ghi vào `customers` và `products`; stage `orders` ghi vào `orders`,
`order_items`, đồng thời cập nhật tồn kho. Stage `payments` hiện chỉ tạo và chạy các
kiểm tra chất lượng (DQ) cho payment records trong bộ nhớ, **chưa ghi records vào
bảng `payments`**.

Generator V2 từ chối chạy lại `master` hoặc `orders` nếu các bảng tương ứng đã có
dữ liệu. Muốn tạo một bộ dữ liệu mới, hãy làm trống database test trước khi chạy lại
từ stage `master`.

Để tạo dữ liệu cho pipeline Docker mặc định (`ecommerce`), vẫn dùng
`source/generator/generate_data.py` hoặc một nguồn dữ liệu khác kết nối database đó.

## 3. Trích xuất PostgreSQL sang tầng Bronze

```powershell
.\.venv\Scripts\python.exe ingestion\postgres_to_minio.py
```

Lệnh này tải một file CSV có timestamp cho mỗi bảng lên bucket
`ecommerce-data-lake`, tại đường dẫn `bronze/<table>/`.

## 4. Kiểm tra tầng Bronze

```powershell
.\.venv\Scripts\python.exe validation\validate_bronze.py
```

Lệnh này so sánh số dòng của CSV Bronze mới nhất với PostgreSQL và kiểm tra khóa
chính bị trùng.

## 5. Chuyển đổi Bronze sang Silver

Chạy Spark trong Spark container của project. Tùy chọn `--packages` bổ sung S3A
connector để truy cập MinIO; lần chạy đầu có thể tải các dependency.

```powershell
docker exec -w /opt/spark/work-dir ecommerce-spark spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 transformation/bronze_to_silver.py
```

Lệnh này đọc CSV Bronze mới nhất của mọi bảng, áp dụng transformation, rồi ghi
đè dataset Parquet tương ứng tại `silver/<table>/`.

## 6. Kiểm tra tầng Silver

Dùng validator tối ưu cho các lần chạy thông thường:

```powershell
docker exec -w /opt/spark/work-dir ecommerce-spark spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 validation/validate_silver_optimized.py
```

Validator đọc `validation/contracts/silver.yml`, kiểm tra schema contract,
rule chất lượng dữ liệu, foreign key, và trả về exit code `1` khi có lỗi.

Nếu muốn chạy validator đơn giản, chưa tối ưu, dùng lệnh sau:

```powershell
docker exec -w /opt/spark/work-dir ecommerce-spark spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 validation/validate_silver.py
```

## 7. Chạy pipeline Bronze bằng Airflow

DAG Airflow hiện tại chạy ingestion và validation Bronze. Khởi động Airflow sau
khi PostgreSQL và MinIO đã sẵn sàng:

```powershell
docker compose run --rm airflow-init
docker compose up -d airflow-api-server airflow-scheduler airflow-dag-processor
```

Mở Airflow tại `http://localhost:8085`, bật DAG `ecommerce_pipeline`, sau đó
trigger thủ công. Lịch chạy định kỳ của DAG là `0 19 * * *`.

## Các lệnh xem trạng thái thường dùng

```powershell
docker compose ps
docker compose logs -f postgres
docker compose logs -f minio
docker logs -f ecommerce-spark
docker compose down
```

`docker compose down` dừng và xóa container nhưng vẫn giữ lại named volume.
