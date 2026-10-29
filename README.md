# Bigdata JobAnalyzer

Nền tảng phân tích thị trường việc làm sử dụng Hadoop HDFS, Apache Spark, FastAPI
và React. Hệ thống nhận dữ liệu việc làm dạng CSV, lưu dữ liệu thô trên HDFS,
chạy pipeline Spark ba tầng, sau đó cung cấp kết quả qua REST API và dashboard web.

## 1. Kiến trúc hệ thống

```text
CSV local / CSV upload
        |
        v
HDFS /raw
        |
        v
Tầng 1 - Ingestion
  Đọc CSV, chuẩn hóa tên cột, thêm metadata, ghi Parquet Snappy
        |
        v
Tầng 2 - Cleaning
  Chuẩn hóa lương, địa lý, kỹ năng, học vấn, kinh nghiệm và thời gian
  Quality Gate + Quarantine
        |
        v
Tầng 3 - Analytics
  Spark SQL + Spark MLlib + JSON Serving Store
        |
        +--> FastAPI REST API :8000
        |
        +--> React/Vite Dashboard :5173
```

Các thư mục chính:

```text
1_infrastructure/   Cấu hình và vận hành HDFS
2_pipeline/         Pipeline Spark Tầng 1, Tầng 2, Tầng 3
3_backend/          FastAPI API và upload pipeline
4_frontend/         React dashboard
etl/                Thư viện chuẩn hóa và xử lý dữ liệu dùng chung
scripts/             Script hỗ trợ chạy, kiểm tra và cài đặt
tests/              Kiểm thử Python
config/             Cấu hình ứng dụng
data/               Serving Store và metadata được sinh khi chạy
```

## 2. Yêu cầu môi trường

Hướng dẫn này dành cho Windows Native. Cần cài:

- Windows 10/11.
- Git.
- Python 3.10 trở lên.
- Java JDK 17.
- Hadoop 3.x có `hdfs.cmd`, `hadoop.cmd` và `winutils.exe`.
- Apache Spark 3.5.x được build cho Hadoop 3.
- Node.js 18 trở lên và npm.
- Ít nhất 8 GB RAM; khuyến nghị 16 GB khi chạy dataset lớn.
- Ít nhất 10 GB dung lượng trống cho Hadoop, Spark và dữ liệu tạm.

Mặc định các script sử dụng:

```text
C:\bigdata\java
C:\bigdata\hadoop
C:\bigdata\spark
```

Nếu cài ở vị trí khác, cập nhật biến môi trường và các script trong
`1_infrastructure/` trước khi chạy.

Kiểm tra công cụ:

```powershell
java -version
python --version
node --version
npm --version
C:\bigdata\hadoop\bin\hdfs.cmd version
C:\bigdata\spark\bin\spark-submit.cmd --version
```

## 3. Lấy mã nguồn và cài dependency

```powershell
git clone https://github.com/AXtremeTS/Bigdata_JobAnalyzer.git
cd Bigdata_JobAnalyzer
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
cd 4_frontend
npm install
cd ..
```

## 4. Chuẩn bị dataset

Dataset lớn không được lưu trong GitHub. Người chạy cần tự cung cấp một file CSV,
ví dụ `VietJobs.csv`, tại thư mục gốc hoặc dùng file CSV riêng khi upload trên giao diện.

Dataset nên có các nhóm thông tin sau, nhưng tên cột có thể khác:

- Chức danh công việc.
- Công ty.
- Mức lương.
- Địa điểm.
- Kinh nghiệm.
- Học vấn.
- Kỹ năng.
- Ngày đăng.

Pipeline tự nhận diện encoding, delimiter và alias cột. Dataset thiếu trường vẫn có
thể chạy; biểu đồ tương ứng sẽ được ghi nhận là không có dữ liệu.

Không commit dataset lớn, file upload tạm, database runtime hoặc secret vào Git.

## 5. Khởi động HDFS

```powershell
.\run_all.ps1 -Step hdfs
.\run_all.ps1 -Step status
```

Kiến trúc HDFS mặc định:

```text
NameNode RPC:      localhost:9000
NameNode Web UI:   http://localhost:9870
DataNode 1:        port 9866
DataNode 2:        port 9876
DataNode 3:        port 9886
Replication:       3
```

Kiểm tra dữ liệu đang lưu:

```powershell
.\run_all.ps1 -Step inventory
```

Nếu là lần chạy đầu tiên, có thể dùng `1_infrastructure/test-hdfs.ps1` để kiểm tra
cluster và nạp file raw sau khi cấu hình xong.

## 6. Chạy pipeline Spark

Sau khi HDFS hoạt động và dataset raw đã sẵn sàng:

```powershell
.\run_all.ps1 -Step pipeline
```

Hoặc:

```powershell
python 2_pipeline/run_pipeline.py
```

Pipeline thực hiện:

1. Tầng 1 đọc CSV từ HDFS raw, chuẩn hóa tên cột và ghi Parquet Snappy vào `/tang1/jobs`.
2. Profiling kiểm tra schema và khả năng ánh xạ cột.
3. Tầng 2 chuẩn hóa dữ liệu, ghi `/tang2/jobs`, cách ly lỗi vào `/quarantine/jobs`.
4. Tầng 3 chạy Spark SQL và ghi kết quả vào `/tang3/jobs`.
5. Spark MLlib chạy Random Forest và K-Means.
6. JSON phục vụ API được ghi vào `data/tang3/`.

Chạy từng tầng:

```powershell
python 2_pipeline/tang1_ingest/tang1_job.py
python 2_pipeline/tang2_cleaning/tang2_job.py
python 2_pipeline/tang3_analytics/tang3_job.py
python 2_pipeline/tang3_analytics/ml.py
```

Các output chính:

```text
HDFS:
/raw/jobs
/tang1/jobs
/tang2/jobs
/tang3/jobs
/quarantine/jobs

Local serving:
data/tang3/overview.json
data/tang3/hot_jobs.json
data/tang3/salary_by_level.json
data/tang3/geo_distribution.json
data/tang3/requirements.json
data/tang3/top_skills.json
data/tang3/top_companies.json
data/tang3/year_distribution.json
data/tang3/month_distribution.json
data/tang3/ml_metrics.json
data/tang3/ml_clusters.json
data/meta/quality.json
data/meta/capabilities.json
```

## 7. Khởi động Backend

Mở PowerShell mới, kích hoạt `.venv`, rồi chạy:

```powershell
python -m uvicorn 3_backend.app.main:app --host 127.0.0.1 --port 8000 --reload
```

Truy cập:

```text
API:       http://localhost:8000
Swagger:   http://localhost:8000/docs
OpenAPI:   http://localhost:8000/openapi.json
```

Endpoint chính:

```text
GET  /api/health
GET  /api/datasets
GET  /api/datasets/{dataset_id}/jobs
GET  /api/datasets/{dataset_id}/insights/{module}
GET  /api/datasets/{dataset_id}/quality
GET  /api/datasets/{dataset_id}/capabilities
POST /api/datasets/{dataset_id}/predict-salary
POST /api/datasets/upload
GET  /api/pipeline/jobs/{job_id}
GET  /api/pipeline/jobs/{job_id}/events
```

## 8. Khởi động Frontend

Mở PowerShell khác:

```powershell
cd 4_frontend
npm run dev
```

Mở `http://localhost:5173`.

Build production:

```powershell
npm run build
```

Dashboard gồm Job Explorer, Analytics, bộ lọc năm/tháng, ML Estimator,
Upload Pipeline và HDFS Cluster.

## 9. Upload dataset từ dashboard

1. Khởi động HDFS, Backend và Frontend.
2. Mở tab Upload Pipeline.
3. Chọn hoặc kéo thả file `.csv`.
4. Backend tạo `dataset_id` và `job_id`.
5. File được staging vào:

   ```text
   /raw/uploads/<dataset_id>/<filename>
   ```

6. Pipeline tạo:

   ```text
   /datasets/<dataset_id>/tang1
   /datasets/<dataset_id>/tang2
   /datasets/<dataset_id>/tang3
   data/datasets/<dataset_id>/tang3
   ```

7. Khi job hoàn thành, dashboard tự tải dataset mới.

File local chỉ là vùng đệm nhận upload; nguồn phân tích chính là HDFS.

## 10. Chạy kiểm thử

```powershell
pytest -q
python -m compileall 1_infrastructure 2_pipeline 3_backend etl tests
cd 4_frontend
npm run build
npm run lint
cd ..
```

## 11. Xử lý lỗi thường gặp

### HDFS không khởi động

Kiểm tra:

```powershell
.\run_all.ps1 -Step status
```

Xác nhận `JAVA_HOME`, `HADOOP_HOME`, `HADOOP_CONF_DIR` và `PATH` trỏ đúng
đến Java/Hadoop. Kiểm tra NameNode Web UI tại `http://localhost:9870`.

### Spark không tìm thấy Java hoặc Hadoop

```powershell
$env:JAVA_HOME = "C:\bigdata\java"
$env:HADOOP_HOME = "C:\bigdata\hadoop"
$env:SPARK_HOME = "C:\bigdata\spark"
$env:PATH = "$env:HADOOP_HOME\bin;$env:SPARK_HOME\bin;$env:JAVA_HOME\bin;$env:PATH"
```

Chạy lại pipeline trong cùng cửa sổ PowerShell.

### Backend mở được nhưng dashboard không có dữ liệu

1. Kiểm tra `http://localhost:8000/docs`.
2. Gọi `http://localhost:8000/api/health`.
3. Kiểm tra `data/tang3/` có JSON chưa.
4. Kiểm tra browser console và CORS.
5. Chạy lại `.\run_all.ps1 -Step pipeline`.

### Dataset thiếu salary, education hoặc application count

Đây là trường hợp hợp lệ. Pipeline giữ dữ liệu có sẵn và metadata capability cho
Frontend. Biểu đồ sẽ hiển thị trạng thái thiếu dữ liệu hoặc dùng số tin đăng thay
cho lượt ứng tuyển, không tự tạo số liệu giả.

### Port đã được sử dụng

Dừng tiến trình đang dùng cổng 8000 hoặc 5173 rồi khởi động lại Backend/Frontend.

## 12. Dừng hệ thống

Đóng Backend và Frontend dev server bằng `Ctrl+C`, sau đó:

```powershell
.\run_all.ps1 -Step stop
```

## 13. Ghi chú về repository

Repository không chứa dataset lớn, thư mục `docs/`, file Markdown phụ trợ, file upload
tạm hoặc database runtime. Người dùng cần tự chuẩn bị dataset và môi trường Hadoop/Spark
trước khi chạy hệ thống.
