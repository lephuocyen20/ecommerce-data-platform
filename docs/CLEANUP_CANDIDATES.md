# Danh sách ứng viên dọn dẹp

Ngày rà soát: 25-10-2026. Đây là đề xuất **không thực hiện xóa**. Mức độ tin cậy
được dựa trên tham chiếu mã nguồn, DAG, README và Runbook hiện có.

| Ưu tiên | Mục / nhóm | Bằng chứng | Điều kiện trước khi xóa | Khuyến nghị |
|---|---|---|---|---|
| 1 — rất an toàn | `airflow/dags/ecommerce_incremental_ingestion.py.bak` | Không có import/runtime reference; Airflow chỉ nạp `.py`; bản hiện hành lớn hơn 463 dòng diff. | Không cần giữ bản backup tại working tree. | **Xóa.** Lịch sử Git đã là backup. |
| 1 — rất an toàn nếu là artifact | `snapshot_builder_test.log` (untracked) | Là file log test, không được mã nguồn tham chiếu. | Xác nhận không cần đính kèm log để debug. | **Xóa / đưa vào `.gitignore`** nếu log phát sinh lại. |
| 2 — xóa theo cụm nếu bỏ batch pipeline cũ | `airflow/dags/ecommerce_pipeline.py`, `ingestion/postgres_to_minio.py`, `validation/validate_bronze.py` | Chỉ DAG `ecommerce_pipeline` gọi `ingest_table`; README gọi nó là “Existing batch pipeline”. `validate_bronze.py` phục vụ Bronze batch/legacy, trong khi incremental có `validate_incremental_bronze.py`. | Team xác nhận chỉ vận hành DAG `ecommerce_incremental_ingestion`, không cần re-run full/batch legacy. Cập nhật README, ARCHITECTURE, RUNBOOK trước/cùng PR. | **Ứng viên tốt để retire**, nhưng không xóa từng file độc lập. |
| 2 — refactor, chưa được xóa trực tiếp | `transformation/bronze_to_silver.py` | Là transform batch cũ và được legacy DAG gọi, nhưng incremental module vẫn import `configure_s3` và `TRANSFORMS` từ file này. Hai validator cũng import `configure_s3`. | Tách `configure_s3` và `TRANSFORMS` sang module chung, đổi import, chạy transform + validation tests; sau đó mới bỏ phần `main`, đọc Bronze/ghi Silver kiểu batch. | **Không xóa nguyên file lúc này.** Có thể giảm nó sau refactor. |
| 3 — validator trùng chức năng | `validation/validate_silver.py` | Không phải task DAG; DAG dùng `validate_silver_optimized.py`. Tuy vậy bản optimized dùng cùng contract và đây vẫn là baseline CLI/readable reference. | So sánh kết quả hai bản trên contract thực tế; chuyển các behavior/test cần thiết sang bản optimized và cập nhật Runbook. | **Cân nhắc hợp nhất**, không nên xóa ngay. |
| 4 — manual integration scripts | `warehouse/loader/test_raw_upsert_rollback.py`, `test_atomic_rollback.py`, `test_warehouse_batch_rollback.py`, `test_watermark_continuity.py`, `test_warehouse_guards.py` | Đây là script integration chạy top-level, phụ thuộc DB/MinIO state và tự rollback. Chúng không được runtime import. Một số intent chồng nhau (guard/continuity, rollback). | Chuyển các scenario cần giữ thành pytest có fixture cô lập; bảo đảm test CI chạy được. | **Nên di chuyển/chuẩn hóa trước**, không xóa để tránh mất regression coverage. |

## Không đưa vào danh sách xóa

- `source/generator/generate_data_v2.py` chỉ dài vài dòng nhưng là entrypoint CLI
  được Runbook gọi; giữ lại.
- Toàn bộ `ingestion/incremental/`, `transformation/incremental_bronze_to_silver.py`,
  `warehouse/loader/` runtime, `validation/validate_incremental_bronze.py` và dbt
  models đều nằm trên luồng incremental hiện tại; không có bằng chứng là dead code.
- `transformation/snapshot/`, các `tests/test_snapshot_*.py` và
  `docs/FUNCTION_REFERENCE.md` hiện là file untracked từ working tree. Chúng chưa
  thuộc commit nền nên không đánh giá là mã dư hay đề xuất xóa.

## Thứ tự dọn dẹp an toàn

1. Xóa backup `.bak` và log artifact sau khi người sở hữu xác nhận.
2. Quyết định chính thức: giữ hay retire batch pipeline cũ. Nếu retire, xóa cả cụm
   legacy và cập nhật documentation trong cùng một thay đổi.
3. Tách shared Spark helpers, rồi mới loại phần transform batch còn lại.
4. Chuyển manual DB tests sang pytest trước khi loại script integration.

## Rủi ro cần nhớ

Xóa một file “không được Python import” vẫn có thể làm hỏng lệnh Docker/Airflow,
Runbook hoặc thao tác vận hành thủ công. Vì vậy mọi mục ưu tiên 2–4 cần kiểm tra
`docker compose`, DAG discovery và test đúng phạm vi sau khi dọn.
