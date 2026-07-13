User Behavior Analytics for Video Streaming Platform (Hệ thống phân tích hành vi người dùng trên nền tảng phát video trực tuyến)
```markdown

Một đường ống (pipeline) phân tích dữ liệu lớn toàn diện (end-to-end) áp dụng **Kiến trúc Hybrid Lambda** để thu nạp, xử lý, lưu trữ và phân tích hàng triệu sự kiện tương tác theo thời gian thực của người dùng (luồng nhấp chuột - clickstream, hành vi phát video, nhật ký tìm kiếm) trên một nền tảng giả lập tương tự như Netflix.

---

🏗️ Kiến trúc hệ thống

Hệ thống được thiết kế dựa trên mô hình **Kiến trúc Lambda**, giúp phân tách độc lập giữa việc xử lý luồng dữ liệu thời gian thực độ trễ thấp và việc phân tích chuyên sâu dữ liệu lịch sử theo mẻ (batch).

*   **Tầng Tốc độ - Speed Layer (Dữ liệu nóng - Hot Data):** Đường ống xử lý độ trễ thấp sử dụng **Spark Structured Streaming** (Chu kỳ Micro-batch 10 giây) để liên tục kéo dữ liệu từ **Apache Kafka**, sau đó đẩy lập tức sang **MongoDB** nhằm cập nhật tức thì cho các biểu đồ trên Dashboard real-time.
*   **Tầng Xử lý theo mẻ - Batch Layer (Dữ liệu lạnh - Cold Data):** Đường ống lưu trữ an toàn, ghi toàn bộ sự kiện thô xuống kho dữ liệu phân tán **Hadoop HDFS** dưới định dạng tệp **Parquet** nén, được tối ưu hóa cho các tác vụ tính toán mẻ hạng nặng thông qua **Spark SQL** và **Spark MLlib**.

Đường ống dữ liệu doanh nghiệp 7 tầng (The 7-Layer Enterprise Data Pipeline)
1.  **Data Source (Tầng nguồn):** Giả lập tương tác của người dùng Netflix (Sử dụng tập dữ liệu Kaggle Dataset với hơn 210.000 bản ghi).
2.  **Access Layer (Tầng truy cập):** Script Python giả lập luồng bắn sự kiện chạy liên tục với tần suất 20 sự kiện/giây nhằm tạo áp lực tải thực tế cho hệ thống.
3.  **Data Ingestion Layer (Tầng thu nạp):** Cụm Apache Kafka Cluster vận hành qua cơ chế **KRaft** (Kiến trúc thế hệ mới không cần ZooKeeper).
4.  **Processing Layer (Tầng xử lý):** Cụm Apache Spark Cluster phân tán (Gồm 1 Node Master và 2 Nodes Worker trực tiếp tính toán).
5.  **Storage Layer (Tầng lưu trữ đa tầng):** Chiến lược lưu trữ kép thông qua **Hadoop HDFS** (Đóng vai trò Data Lake) & **MongoDB** (Đóng vai trò NoSQL Serving Layer để truy xuất nhanh).
6.  **Analytics & ML Layer (Tầng phân tích nâng cao & Học máy):** Tổng hợp các báo cáo vận hành bằng **Spark SQL (Mô hình Star Schema)** và chạy thuật toán phân cụm khách hàng phân tán bằng **Spark MLlib (Thuật toán K-Means)**.
7.  **Serving Layer (Tầng hiển thị):** Ứng dụng giao diện trực quan **Streamlit Dashboard App** với 3 Tab chuyên biệt (Real-time, Báo cáo vận hành, Chiến lược AI).

---

Ma trận công nghệ ứng dụng (Tech Stack)

| Thành phần | Công nghệ | Vai trò trong hạ tầng |
| :--- | :--- | :--- |
| **Ảo hóa hạ tầng** | Docker / Docker Compose | Khởi tạo, cô lập tài nguyên và điều phối cụm phân tán đa nút |
| **Hàng đợi thông điệp** | Apache Kafka (KRaft) | Nền tảng luân chuyển luồng sự kiện phân tán, hiệu năng cao |
| **Động cơ xử lý chính**| Apache Spark (Streaming) | Thực hiện quy trình biến đổi dữ liệu, Stream-Static Join trực tiếp trên RAM |
| **Hồ dữ liệu (Data Lake)** | Hadoop HDFS (YARN) | Hệ thống lưu trữ phân tán, mở rộng quy mô lớn cho các tệp Parquet lịch sử |
| **Cơ sở dữ liệu NoSQL** | MongoDB | Tầng phục vụ dữ liệu nóng (Serving Layer) tốc độ cao cho ứng dụng |
| **Trí tuệ nhân tạo (AI)** | Spark MLlib | Huấn luyện mô hình phân cụm toán học K-Means phân tán trên các Node |
| **Kho dữ liệu (Data Warehouse)** | Spark SQL | Tổ chức cấu trúc dữ liệu theo mô hình hình sao (Bảng Fact & Bảng Dimension) |
| **Giao diện trực quan** | Streamlit | Kết xuất giao diện Dashboard (Tab Real-time, Tab Operational, Tab AI Strategy) |

---

Cấu trúc thư mục mã nguồn

```directory
├── docker-compose.yml             # Bản thiết kế hạ tầng mã hóa (Định nghĩa 9 containers phân tán)
├── simulator_stream.py            # Kafka Producer (Chuyển đổi file CSV tĩnh thành luồng sự kiện 20 eps)
├── spark_streaming_processor.py   # Trái tim xử lý luồng (Micro-batching, ghi kép song song sang HDFS/Mongo)
├── warehouse_builder.py           # Kiến trúc sư Kho dữ liệu (Chuẩn hóa Data Lake thô thành Star Schema)
├── analytics_engine.py            # Công cụ phân tích BI (Chạy các tác vụ Spark SQL tổng hợp chỉ số vận hành)
├── ml_user_segmentation.py        # Tác vụ học máy (Huấn luyện mô hình gom cụm K-Means phân tán)
├── app_dashboard.py               # Ứng dụng Streamlit (Kéo dữ liệu đã tính toán sẵn từ MongoDB để hiển thị)
└── data/                          # Thư mục chứa các tệp CSV nguồn từ Kaggle (users, movies, logs)

```

---

## 💾 Mô hình hóa dữ liệu (Sơ đồ hình sao - Star Schema)

Dữ liệu lịch sử lưu trữ trên HDFS được cấu trúc lại thành một Kho dữ liệu (Data Warehouse) tối ưu theo mô hình **Sơ đồ hình sao (Star Schema)** nhằm tăng tốc tối đa cho các câu lệnh truy vấn `JOIN` phân tán:

* **Các bảng chiều (Dimension Tables):** `dim_users` (Thông tin nền người dùng / gói cước dịch vụ), `dim_movies` (Danh mục phim / điểm số IMDb).
* **Các bảng sự kiện (Fact Tables):** `fact_watch_history` (Nhật ký xem phim chi tiết, tiến độ xem %), `fact_search_logs` (Hành vi từ khóa tìm kiếm, nhãn gõ sai chính tả).

---

## 🚀 Hướng dẫn khởi chạy & Triển khai

### Điều kiện tiên quyết

* Máy tính đã cài đặt Docker & Docker Compose.
* Cấp phát tài nguyên RAM tối thiểu cho Docker Engine là 8GB (Khuyến khích từ 12GB trở lên vì hệ thống chạy cụm HDFS và cụm Spark phân tán).

### Bước 1: Khởi động toàn bộ hạ tầng

Dựng toàn bộ hệ thống cụm phân tán gồm 9 thùng chứa (containers) độc lập:

```bash
docker compose up -d

```

### Bước 2: Kích hoạt đường ống xử lý luồng thời gian thực

1. **Chạy bộ xử lý Spark Streaming:** Khởi tạo Spark để bắt đầu lắng nghe cổng Kafka và mở hai nhánh ghi dữ liệu.
```bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /app/spark_streaming_processor.py

```


2. **Kích hoạt Script giả lập nguồn:** Bắt đầu bắn luồng dữ liệu hành vi người dùng vào các Kafka Topics.
```bash
python simulator_stream.py

```



### Bước 3: Chạy các tác vụ phân tích mẻ (Batch) và Học máy (AI)

Sau khi dữ liệu thô đã được tích lũy một lượng nhất định trong Data Lake (HDFS), tiến hành chạy các tiến trình xử lý định kỳ:

```bash
# 1. Cấu trúc lại dữ liệu thô thành Kho dữ liệu Star Schema
docker exec -it spark-master spark-submit /app/warehouse_builder.py

# 2. Chạy báo cáo BI (Rủi ro rời bỏ do nghẽn mạng, Khung giờ cao điểm, Từ khóa lỗi)
docker exec -it spark-master spark-submit /app/analytics_engine.py

# 3. Kích hoạt mô hình AI phân cụm người dùng K-Means phân tán
docker exec -it spark-master spark-submit /app/ml_user_segmentation.py

```

### Bước 4: Mở giao diện Dashboard hiển thị

Khởi chạy ứng dụng trực quan Streamlit:

```bash
streamlit run app_dashboard.py

```

Truy cập trình duyệt theo đường dẫn `http://localhost:8501` để theo dõi các chỉ số đo lường thời gian thực và phân khúc khách hàng.

---

## 🧠 Những đánh đổi trong thiết kế & Lưu ý khi triển khai thực tế (Production Considerations)

Hệ thống này được xây dựng dựa trên tư duy **Thiết kế kiến trúc phòng thủ (Defensive Design)**, thẳng thắn nhìn nhận các hạn chế vật lý của môi trường phân tán:

1. **Bài toán Ghi kép (The Dual-Write Dilemma):** Spark thực hiện ghi đồng thời sang cả HDFS và MongoDB bên trong hàm `.foreachBatch()`. Trên môi trường thực tế, hiện tượng nghẽn mạng cục bộ có thể dẫn đến việc một bên ghi thành công, một bên thất bại gây mất đồng bộ dữ liệu. *Giải pháp khắc phục: Trong tương lai sẽ chuyển dịch sang kiến trúc thống nhất Lakehouse (như Apache Iceberg hoặc Delta Lake) nằm trên HDFS để đảm bảo giao dịch ACID, sau đó dùng cơ chế CDC (Change Data Capture) để đồng bộ bất đồng bộ sang MongoDB.*
2. **Thảm họa file nhỏ (Small Files Problem):** Việc ép cơ chế Micro-batch chạy liên tục sau mỗi 10 giây sẽ tạo ra vô số file Parquet dung lượng rất nhỏ (vài KB) trên HDFS, gây tràn bộ nhớ RAM của NameNode về lâu dài. *Giải pháp khắc phục: Thiết lập một Compaction Job chạy ngầm định kỳ vào 2 giờ sáng hàng ngày để tự động gom (Merge) các file vài KB này thành các khối Block chuẩn 128MB của HDFS.*
3. **Kiểm soát áp lực ngược (Backpressure Control):** Các Node Worker trong cụm Spark của đồ án bị giới hạn tài nguyên (1 Core, 1GB RAM mỗi Node). Để hệ thống không bị quá tải dẫn đến sập bộ nhớ (lỗi OOM) khi lượng dữ liệu từ Kafka tăng đột biến (Data Spike), thuộc tính `spark.streaming.backpressure.enabled` đã được cấu hình để Spark tự động tiết lưu, giảm tốc độ kéo dữ liệu dựa trên hiệu năng thực tế của các Worker.
4. **Hạn chế của cụm Kafka KRaft đơn nút:** Để phù hợp với tài nguyên máy cá nhân chạy demo, Kafka hiện tại được gom trong 1 container duy nhất. Đối với môi trường doanh nghiệp thực tế, hệ thống cần được mở rộng cấu hình thành cụm Multi-Broker (tối thiểu 3 Nodes) kết hợp tính năng `Rack Awareness` (Nhận biết vị trí tủ đĩa) và tách biệt các `Quorum Controller Nodes` để đảm bảo tính Sẵn sàng cao (High Availability) và khả năng Chịu lỗi vật lý (Fault Tolerance).

```

```
