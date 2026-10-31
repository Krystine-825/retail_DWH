# 🛒 Retail Data Warehouse (Medallion Architecture)

## 📌 Tổng quan dự án
Dự án này xây dựng một Data Warehouse hiện đại cho một doanh nghiệp bán lẻ (mỹ phẩm/nước hoa) cỡ vừa và nhỏ (SME). Hệ thống thu thập, làm sạch và tổng hợp dữ liệu doanh thu từ 3 kênh bán hàng độc lập (Website Magento, Shopee, và POS tại cửa hàng vật lý) để phục vụ cho nhu cầu phân tích và trực quan hóa dữ liệu (BI).

## 🛠 Tech Stack
* **Orchestration:** Apache Airflow (Dockerized)
* **Data Warehouse:** PostgreSQL
* **Data Transformation:** dbt (data build tool) - dbt-core & dbt-postgres
* **Data Ingestion:** Python (Pandas, SQLAlchemy)
* **Infrastructure:** Docker & Docker Compose

## 🏗 Kiến trúc hệ thống (Medallion Architecture)
Dự án áp dụng mô hình kiến trúc Medallion 3 lớp chuẩn mực để đảm bảo chất lượng và logic dữ liệu:

1. **Bronze Layer (Raw Data):** 
   * Lưu trữ dữ liệu thô (từ các file CSV định kỳ) nguyên bản từ các nguồn.
   * *Công nghệ:* Airflow PythonOperator + Pandas `to_sql`.
2. **Silver Layer (Cleansed & Conformed):** 
   * Làm sạch, chuẩn hóa kiểu dữ liệu, đồng nhất các trường dữ liệu bị lệch (VD: format ngày tháng lộn xộn, mã SKU chứa tiền tố `SP-`, trạng thái đơn hàng tiếng Việt/Anh).
   * *Công nghệ:* dbt staging models (`stg_website_orders`, `stg_shopee_orders`, `stg_pos_transactions`).
3. **Gold Layer (Curated for Business):** 
   * Tổng hợp dữ liệu đã làm sạch thành bảng Fact phục vụ phân tích. Sử dụng `UNION ALL` để gộp doanh thu 3 kênh về chung một cấu trúc chuẩn.
   * *Công nghệ:* dbt marts models (`fact_sales`).

## 🚀 Hướng dẫn khởi chạy (Local Development)

### 1. Khởi tạo hạ tầng (Docker)
```bash
docker-compose up -d
```
Hệ thống sẽ chạy các container: Postgres (Database), pgAdmin (UI), Airflow Webserver, Scheduler, và Init.

### 2. Thiết lập Database
Truy cập container Postgres và tạo database `retail_dwh`:
```bash
docker exec -it retail_data_warehouse-postgres-1 psql -U airflow -c "CREATE DATABASE retail_dwh;"
```

### 3. Nạp dữ liệu thô (Ingestion)
* Truy cập Airflow UI: `http://localhost:8080` (admin/admin).
* Bật và Trigger DAG `ingest_to_bronze_layer` để nạp dữ liệu từ file `.csv` vào schema `bronze`.

### 4. Transform dữ liệu (dbt)
Kích hoạt môi trường ảo (venv) và chạy dbt:
```powershell
.\venv\Scripts\activate
cd dbt_project
dbt run
```
Dbt sẽ tự động tạo schema `silver` (chứa các view làm sạch) và schema `gold` (chứa bảng `fact_sales`).

---

## 💡 Bài học kinh nghiệm & Xử lý sự cố (Troubleshooting)

Trong quá trình xây dựng pipeline, tôi đã gặp và giải quyết các vấn đề kỹ thuật sau:

### 1. Lỗi Dependency Hell khi khởi tạo Airflow
* **Vấn đề:** Container `airflow-init` bị crash (exit 1) và timeout do xung đột thư viện giữa `dbt-postgres` và core của Airflow (cùng dùng chung các gói Jinja2, Click) khi cố nhồi chung vào biến `_PIP_ADDITIONAL_REQUIREMENTS`.
* **Giải pháp:** Tách biệt môi trường. Airflow container chỉ cài các gói cần cho Ingestion (Pandas, SQLAlchemy). Cài đặt `dbt` ở một môi trường ảo (Virtual Environment) độc lập bên ngoài host máy tính.

### 2. Lỗi SQLAlchemy Transaction Commit
* **Vấn đề:** Task Airflow thất bại với lỗi `AttributeError: 'Connection' object has no attribute 'commit'`.
* **Nguyên nhân:** Phiên bản SQLAlchemy 2.0+ đã thay đổi cách quản lý transaction, không cho phép gọi hàm `conn.commit()` trực tiếp từ object connection thông thường.
* **Giải pháp:** Đổi `engine.connect()` thành `engine.begin()`. Context manager `begin()` sẽ tự động quản lý transaction và commit khi khối lệnh hoàn tất.

### 3. Lỗi tạo sai tên Schema của dbt
* **Vấn đề:** Khi cấu hình schema `gold` trong `dbt_project.yml`, dbt tự động sinh ra các schema có tên kỳ lạ như `silver_gold` và `silver_silver`.
* **Nguyên nhân:** Hành vi mặc định của dbt là nối tên schema mặc định (trong `profiles.yml`) với tên custom schema.
* **Giải pháp:** 
  1. Viết đè macro `generate_schema_name` bằng cách tạo file `macros/generate_schema_name.sql` để ép dbt sử dụng đúng tên được cung cấp mà không nối chuỗi.
  2. Sử dụng lệnh `DROP SCHEMA ... CASCADE` trong pgAdmin để dọn dẹp các schema rác trước khi chạy lại dbt.

### 4. Lỗi Connection Port (Postgres Local vs Docker)
* **Vấn đề:** dbt báo lỗi `FATAL: password authentication failed for user "airflow"` dù cấu hình đúng mật khẩu.
* **Nguyên nhân:** Cổng 5432 bị trùng với Postgres cài sẵn trên máy Windows.
* **Giải pháp:** Đổi port mapping trong `docker-compose.yml` thành `5433:5432` và cấu hình lại `profiles.yml` trỏ về cổng `5433` để đâm chính xác vào DB của Docker.