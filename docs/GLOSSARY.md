# Thuật ngữ — E-Commerce Data Platform

Tài liệu này định nghĩa các khái niệm được sử dụng trong dự án. Luồng dữ liệu
chính là: `PostgreSQL nguồn → Bronze/MinIO → Silver/Parquet → Warehouse → dbt marts`.

## Các lớp dữ liệu

| Thuật ngữ | Định nghĩa trong project |
|---|---|
| **Source / OLTP** | PostgreSQL chứa dữ liệu giao dịch gốc của e-commerce: customers, products, orders, order_items, payments và shipments. |
| **Bronze layer** | Vùng dữ liệu thô trên MinIO. Dữ liệu được đưa lên theo batch, giữ thông tin gốc để truy vết và phục hồi. |
| **Silver layer** | Vùng dữ liệu đã được Spark chuẩn hoá, kiểm tra và ghi dưới dạng Parquet. Đây là đầu vào của warehouse loader. |
| **Warehouse raw** | Các bảng PostgreSQL nhận dữ liệu Silver đã được xác minh; là lớp đầu vào cho dbt. |
| **Staging model** | dbt view chuẩn hoá tên cột, kiểu dữ liệu và quy ước dữ liệu từ warehouse raw. |
| **Data mart** | Lớp phục vụ phân tích gồm dimensions (`dim_customers`, `dim_products`, `dim_date`) và facts (`fact_orders`, `fact_order_items`, `fact_payments`, `fact_shipments`). |

## Xử lý incremental và độ tin cậy

| Thuật ngữ | Định nghĩa trong project |
|---|---|
| **Incremental ingestion** | Chỉ trích xuất các bản ghi mới hoặc thay đổi kể từ vị trí đã xử lý trước đó, thay vì đọc lại toàn bộ bảng nguồn. |
| **Watermark** | Mốc xử lý cuối của một bảng, dùng `timestamp` hoặc `id` để xác định phạm vi dữ liệu cần lấy cho batch tiếp theo. |
| **Batch** | Một đơn vị dữ liệu được xử lý cùng nhau, có `batch_id`, bảng nguồn, watermark đầu/cuối, số dòng, object key và checksum. |
| **Batch metadata** | Metadata lưu cho batch, giúp xác định trạng thái, đối chiếu object MinIO và phục vụ recovery. |
| **Checkpoint** | Bản ghi xác nhận một batch đã được xử lý thành công ở một bước. Checkpoint giúp không xử lý lại hoặc không tiến watermark sai. |
| **PENDING → UPLOADED → COMMITTED** | Chuỗi trạng thái ingestion: batch đã đăng ký nhưng chưa upload; đã upload và xác minh object; cuối cùng đã commit checkpoint/watermark. |
| **Recovery** | Quy trình tiếp tục hoặc xác minh lại batch bị gián đoạn, chỉ cho phép đi tiếp khi dữ liệu và metadata khớp nhau. |
| **Idempotency** | Khả năng chạy lại một thao tác mà không tạo dữ liệu trùng hoặc làm thay đổi kết quả đúng đã có. Project dùng batch ID, manifest và checksum để hỗ trợ điều này. |
| **Checksum** | Giá trị băm SHA-256 dùng để xác minh file/object chưa bị thay đổi giữa các bước xử lý. |
| **Manifest** | Tài liệu metadata liệt kê tập object, batch, schema, row count hoặc watermark thuộc một lần xử lý/snapshot. |
| **Snapshot** | Ảnh chụp Silver có định danh bất biến, gồm các Parquet objects và manifest; dùng để publish/verify dữ liệu theo một trạng thái xác định. |

## Biến đổi và kiểm tra chất lượng

| Thuật ngữ | Định nghĩa trong project |
|---|---|
| **Spark transformation** | Bước dùng Apache Spark để đọc Bronze, áp dụng transform theo từng entity và ghi Silver Parquet. |
| **Parquet** | Định dạng file cột cho Silver, phù hợp cho đọc phân tích và xử lý Spark. |
| **Data contract** | Tập quy tắc trong `validation/contracts/silver.yml`: schema, primary key, not-null, unique, giá trị dương/không âm và foreign key. |
| **Schema validation** | Kiểm tra cột và kiểu dữ liệu có đúng hợp đồng Silver hay không. |
| **Primary key** | Cột định danh duy nhất của mỗi bản ghi, ví dụ `order_id` cho bảng orders. |
| **Foreign key validation** | Kiểm tra quan hệ tham chiếu hợp lệ, ví dụ `orders.customer_id` phải tồn tại trong customers. |
| **Data quality validation** | Tập kiểm tra schema, null, uniqueness, ràng buộc số học và quan hệ dữ liệu trước/sau transform. |

## Warehouse, dbt và điều phối

| Thuật ngữ | Định nghĩa trong project |
|---|---|
| **Upsert** | Ghi dữ liệu theo kiểu insert bản ghi mới hoặc update bản ghi đã tồn tại, dùng khi load Silver vào warehouse. |
| **Transaction / rollback** | Nhóm thao tác database được commit cùng nhau; khi có lỗi, rollback để không để lại trạng thái ghi dở dang. |
| **Warehouse guard** | Cơ chế yêu cầu `WAREHOUSE_WRITE_ENABLED=true` trước khi ghi warehouse. |
| **DRY_RUN** | Chế độ mặc định của DAG incremental: kiểm tra cấu hình và luồng xử lý nhưng không ghi Bronze, Silver hay Warehouse. |
| **WRITE mode** | Chế độ ghi có kiểm soát; yêu cầu chọn bảng tường minh và bật `INGESTION_WRITE_ENABLED=true`. |
| **Airflow DAG** | Workflow có hướng, dùng để điều phối các task ingestion, validation, Spark, warehouse loader và dbt. |
| **DockerOperator** | Airflow operator chạy Spark hoặc dbt trong Docker container riêng. |
| **dbt build** | Lệnh dbt tạo models và chạy tests liên quan, chỉ chạy sau khi warehouse loader hoàn thành. |

## Các entity nghiệp vụ

| Entity | Ý nghĩa |
|---|---|
| **customers** | Khách hàng. |
| **products** | Danh mục sản phẩm và tồn kho. |
| **orders** | Đơn hàng của khách hàng. |
| **order_items** | Các dòng sản phẩm thuộc từng đơn hàng. |
| **payments** | Giao dịch và trạng thái thanh toán của đơn hàng. |
| **shipments** | Thông tin vận chuyển, tracking và trạng thái giao hàng. |

