# ============================================================
# FILE: ml_user_segmentation.py
# PIPELINE MÁY HỌC (Spark MLlib)
#
# HỆ THỐNG PHÂN TÍCH VÀ PHÂN CỤM HÀNH VI NGƯỜI DÙNG XEM VIDEO
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import logging
import sys

# ============================================================
# 1. CẤU HÌNH HỆ THỐNG & LOGGING
# ============================================================

# Thiết lập Log để theo dõi tiến trình chạy của Pipeline trong Terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("SPARK_ML")

# Đường dẫn kết nối hệ thống lưu trữ dữ liệu lớn HDFS
HDFS_ROOT = "hdfs://namenode:9000"
WAREHOUSE_PATH = f"{HDFS_ROOT}/user/hadoop/warehouse"

# Đường dẫn kết nối cơ sở dữ liệu MongoDB (Serving Layer phục vụ Dashboard)
MONGO_URI = "mongodb://admin:secret@mongodb:27017"
DATABASE = "streaming_analytics"

# ============================================================
# 2. KHỞI TẠO SPARK SESSION
# ============================================================

def create_spark_session():
    """
    Khởi tạo và cấu hình Spark Session tích hợp sẵn trình ghi dữ liệu MongoDB Connector.
    """
    logger.info("Đang khởi tạo Spark Session...")

    spark = (
        SparkSession.builder
        .appName("VideoStreaming-ML-Pipeline")
        # Cấu hình URI và Tên Database mặc định cho MongoDB Connector
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.database", DATABASE)
        # Bật tính năng tối ưu hóa thực thi truy vấn thích ứng (AQE) của Spark SQL
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    # Ẩn bớt các Log INFO không cần thiết của Spark, chỉ hiện cảnh báo (WARN) và lỗi (ERROR)
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# 3. TRÍCH XUẤT & BIẾN ĐỔI ĐẶC TRƯNG (FEATURE ENGINEERING)
# ============================================================

def extract_user_features(spark):
    """
    Đọc dữ liệu từ HDFS Data Warehouse, thực hiện gom nhóm SQL để tạo các chỉ số hành vi
    """
    logger.info("Bắt đầu bước Feature Engineering...")

    # Đọc bảng Sự kiện (Fact) và bảng Lịch (Dimension) từ định dạng Parquet trên HDFS
    fact_watch = spark.read.parquet(f"{WAREHOUSE_PATH}/fact_watch_history")
    dim_date = spark.read.parquet(f"{WAREHOUSE_PATH}/dim_date")

    # Đăng ký thành các bảng ảo để viết truy vấn bằng cú pháp SQL thuần túy
    fact_watch.createOrReplaceTempView("fact_watch")
    dim_date.createOrReplaceTempView("dim_date")

    # Thực hiện câu lệnh SQL để tổng hợp 4 đặc trưng cốt lõi của mỗi người dùng
    query = """
        SELECT
            f.user_id,
            -- 1. Tổng số lượt bấm xem phim
            COUNT(f.session_id) * 1.0 AS total_views,
            
            -- 2. Tổng thời lượng xem (tính bằng phút)
            SUM(f.watch_duration_minutes) * 1.0 AS total_duration_minutes,
            
            -- 3. Tiến trình xem trung bình (User thường xem hết phim hay hay tắt giữa chừng)
            AVG(f.progress_percentage) AS avg_progress_percentage,
            
            -- 4. Tỷ lệ xem phim vào ban đêm (từ 22h đêm - 4h sáng hôm sau)
            SUM(CASE WHEN d.is_night = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS night_view_ratio

        FROM fact_watch f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY f.user_id
        
    """

    df = spark.sql(query)

    # Điền giá trị 0.0 vào các ô dữ liệu trống (Null) nếu có để tránh lỗi thuật toán toán học
    df = df.na.fill(0.0)

    logger.info(f"Tổng số lượng người dùng đủ điều kiện đưa vào phân cụm: {df.count()}")
    return df


# ============================================================
# 4. TÌM SỐ LƯỢNG CỤM TỐI ƯU (ELBOW / SILHOUETTE METHOD)
# ============================================================

def find_optimal_k(scaled_df, max_k=8):
    """
    Hàm bổ trợ quét qua các giá trị K từ 2 đến max_k để tính toán điểm Silhouette Score.
    Giúp chứng minh khoa học lý do tại sao chọn K=3 trong báo cáo bài tập lớn.
    """
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction",
        metricName="silhouette"
    )

    logger.info("========== TIẾN TRÌNH KHẢO SÁT CHỈ SỐ K TỐI ƯU ==========")

    for k in range(2, max_k + 1):
        # Huấn luyện nhanh mô hình KMeans với số cụm k
        model = KMeans(
            featuresCol="features",
            predictionCol="prediction",
            k=k,
            seed=44
        ).fit(scaled_df)

        # Dự đoán phân cụm thử nghiệm
        prediction = model.transform(scaled_df)

        # In kết quả khảo sát ra Terminal
        # - Cost: Tổng bình phương khoảng cách tới tâm (Càng nhỏ càng tốt)
        # - Silhouette: Độ tách biệt giữa các cụm (Càng gần 1.0 càng hoàn hảo)
        logger.info(
            f"Thử nghiệm K={k} | "
            f"Cost (Inertia)={model.summary.trainingCost:.2f} | "
            f"Silhouette Score={evaluator.evaluate(prediction):.4f}"
        )


# ============================================================
# 5. HUẤN LUYỆN KMEANS & GÁN NHÃN ĐỘNG (DYNAMIC LABELING)
# ============================================================

def build_and_run_kmeans(features_df):
    """
    Chuẩn hóa dữ liệu, thực hiện phân cụm bằng KMeans, tự động phân tích tâm cụm thực tế 
    để gán nhãn nghiệp vụ một cách linh hoạt, chống lỗi hoán đổi nhãn khi chạy lại pipeline.
    """
    logger.info("Đang gom nhóm các trường đặc trưng thành Vector...")
    feature_cols = ["total_views", "total_duration_minutes", "avg_progress_percentage", "night_view_ratio"]

    # Gom các cột đặc trưng riêng lẻ thành một cột Vector duy nhất mang tên 'raw_features'
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    assembled = assembler.transform(features_df)

    logger.info("Đang tiến hành chuẩn hóa dữ liệu (StandardScaler)...")
    # SỬA ĐỔI TOÁN HỌC: Bật both withStd=True và withMean=True nhằm đưa dữ liệu về phân phối chuẩn tâm 0.
    # Tránh việc cột có giá trị lớn (thời lượng xem) áp đảo các cột có giá trị nhỏ (tỷ lệ xem đêm).
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    scaled = scaler.fit(assembled).transform(assembled)

    # Chạy hàm khảo sát tìm K tối ưu để ghi log theo dõi (Phục vụ phân tích tài liệu bài tập lớn)
    find_optimal_k(scaled)

    logger.info("Đang chạy thuật toán huấn luyện KMeans chính thức với K=3...")
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster_id",
        k=3,
        seed=42
    )
    model = kmeans.fit(scaled)
    predictions = model.transform(scaled)

    logger.info("Báo cáo sơ bộ số lượng phần tử phân bổ trong mỗi mã cụm:")
    predictions.groupBy("cluster_id").count().show()

    # --------------------------------------------------------
    # BƯỚC ĐẮC ĐỊA: TÍNH TÂM CỤM TRÊN DỮ LIỆU GỐC (GIẢI MÃ TÂM CỤM)
    # --------------------------------------------------------
    # Giải pháp tính trung bình hình học trực tiếp từ dữ liệu chưa scale giúp Dashboard hiển thị
    # đúng con số thực tế (phút, phần trăm, lượt xem) thay vì các con số Z-score vô nghĩa.
    centers = (
        predictions
        .groupBy("cluster_id")
        .agg(
            F.avg("total_views").alias("total_views_center"),
            F.avg("total_duration_minutes").alias("duration_center"),
            F.avg("avg_progress_percentage").alias("progress_center"),
            F.avg("night_view_ratio").alias("night_ratio_center")
        )
    )

    # Lưu thông tin tọa độ tâm cụm thực tế này xuống MongoDB làm metadata tham chiếu nếu cần
    centers.write \
        .format("mongodb") \
        .option("collection", "ml_cluster_centroids") \
        .mode("overwrite") \
        .save()

    # --------------------------------------------------------
    # KIẾN TRÚC CHUẨN: GÁN NHÃN ĐỘNG NGAY TẠI TẦNG SPARK
    # --------------------------------------------------------
    # Đưa tập tâm cụm rất nhỏ (3 dòng) về Driver dưới dạng mảng Python để xử lý logic gán nhãn chữ
    center_list = centers.collect()

    # Định danh cụm "Cú đêm": Cụm nào có giá trị trung bình tỉ lệ xem đêm lớn nhất
    night_cluster = max(
        center_list,
        key=lambda x: x["night_ratio_center"]
    )["cluster_id"]

    # Lọc cụm Cú đêm ra, xét 2 cụm còn lại để tìm cụm "Mọt phim"
    remain = [x for x in center_list if x["cluster_id"] != night_cluster]
    
    # Định danh cụm "Mọt phim": Cụm có thời lượng xem (duration) trung bình lớn nhất trong nhóm còn lại
    movie_cluster = max(
        remain,
        key=lambda x: x["duration_center"]
    )["cluster_id"]

    # Sử dụng hàm điều kiện cấu trúc dạng cây (when - otherwise) của Spark để dán nhãn chữ trực tiếp
    labeled = (
        predictions
        .withColumn(
            "segment_name",
            F.when(F.col("cluster_id") == night_cluster, "Cú đêm")
            .when(F.col("cluster_id") == movie_cluster, "Mọt phim")
            .otherwise("Xem giải trí")
        )
    )

    return labeled


# ============================================================
# 6. XUẤT DỮ LIỆU SANG MONGODB (SERVING LAYER)
# ============================================================

def save_segments_to_mongodb(df):
    """
    Ép kiểu dữ liệu an toàn, xử lý triệt để lỗi đổi tên CAST của Spark bằng hàm .alias(),
    sau đó ghi đè dữ liệu hồ sơ khách hàng hoàn chỉnh xuống MongoDB.
    """
    logger.info("Đang chuẩn bị xuất dữ liệu phân cụm hoàn chỉnh sang MongoDB...")

    # GIẢI PHÁP FIX LỖI DASHBOARD: Sử dụng .alias() rõ ràng cho từng cột sau khi .cast("double").
    # Giúp bảo toàn tên gốc của trường dữ liệu, Streamlit Dashboard đọc lên không bị dính lỗi Crash.
    final_df = (
        df.select(
            "user_id",
            F.col("total_views").cast("double").alias("total_views"),
            F.col("total_duration_minutes").cast("double").alias("total_duration_minutes"),
            F.col("avg_progress_percentage").cast("double").alias("avg_progress_percentage"),
            F.col("night_view_ratio").cast("double").alias("night_view_ratio"),
            "cluster_id",
            "segment_name"
        )
    )

    # Ghi dữ liệu xuống collection 'ml_user_segments' trong MongoDB dưới chế độ 'overwrite' (ghi đè mới)
    final_df.write \
        .format("mongodb") \
        .option("collection", "ml_user_segments") \
        .mode("overwrite") \
        .save()

    logger.info("🏆 Đã xuất toàn bộ hồ sơ phân cụm người dùng xuống MongoDB thành công!")


# ============================================================
# 7. HÀM ĐIỀU PHỐI CHÍNH (MAIN FUNCTION)
# ============================================================

def main():
    # Khởi tạo môi trường tính toán Spark
    spark = create_spark_session()

    try:
        logger.info("===== KÍCH HOẠT PIPELINE MÁY HỌC PHÂN CỤM USER =====")
        
        # Bước 1: Trích xuất và dọn dẹp biến số đặc trưng hành vi
        features = extract_user_features(spark)
        
        # Bước 2: Chuẩn hóa, chạy KMeans và tự động dán nhãn nghiệp vụ thông minh
        result = build_and_run_kmeans(features)
        
        # Bước 3: Lưu trữ kết quả đầu ra sạch sẽ phục vụ hiển thị Dashboard
        save_segments_to_mongodb(result)
        
        logger.info("===== KẾT THÚC PIPELINE: THÀNH CÔNG RỰC RỠ =====")

    except Exception as e:
        # Bắt toàn bộ các lỗi phát sinh (thiếu file, lỗi kết nối DB, sai cú pháp) để ghi log hệ thống
        logger.error(f"❌ Pipeline gặp sự cố nghiêm trọng: {e}")
        sys.exit(1)

    finally:
        # Đảm bảo tắt Spark Session giải phóng tài nguyên RAM/CPU cho Cluster dù pipeline thành công hay thất bại
        spark.stop()
        logger.info("Đã đóng kết nối giải phóng tài nguyên Spark.")


if __name__ == "__main__":
    main()