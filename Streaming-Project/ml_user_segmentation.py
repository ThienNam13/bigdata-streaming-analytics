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
from logging.handlers import RotatingFileHandler
import logging
import sys
import os
# 1. CẤU HÌNH HỆ THỐNG & LOGGING
LOG_DIR = "/opt/spark/logs"

try:
    os.makedirs(LOG_DIR, exist_ok=True)
except FileExistsError:
    # Nếu Docker báo lỗi "File exists" giả lập do cơ chế Mount, bỏ qua một cách an toàn
    pass

LOG_FILE_PATH = os.path.join(LOG_DIR, "ml_segmentation.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        # Giới hạn dung lượng file log tối đa 20MB, lưu tối đa 5 file backup
        RotatingFileHandler(LOG_FILE_PATH, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SPARK_ML")

# Đường dẫn kết nối hệ thống lưu trữ dữ liệu lớn HDFS
HDFS_ROOT = "hdfs://namenode:9000"
WAREHOUSE_PATH = f"{HDFS_ROOT}/user/hadoop/warehouse"

# Đường dẫn kết nối cơ sở dữ liệu MongoDB (Serving Layer phục vụ Dashboard)
MONGO_URI = "mongodb://admin:secret@mongodb:27017"
DATABASE = "streaming_analytics"

# UTILS: HÀM TRÍCH XUẤT BẢNG SPARK THÀNH CHUỖI ĐỂ GHI LOG
def get_show_string(df, n=5):
    """Chuyển đổi n dòng của DataFrame thành chuỗi ký tự dạng bảng sạch để ghi vào log file"""
    try:
        return df._jdf.showString(n, 25, False)
    except Exception:
        return "Không thể hiển thị mẫu dữ liệu."
    
# 2. KHỞI TẠO SPARK SESSION

def create_spark_session():
    logger.info("Đang khởi tạo Spark Session...")
    spark = (
        SparkSession.builder
        .appName("VideoStreaming-ML-Pipeline")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.database", DATABASE)
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
    logger.info("Bắt đầu bước Feature Engineering...")

    # Đọc bảng Sự kiện (Fact) và bảng Lịch (Dimension) từ định dạng Parquet trên HDFS
    fact_watch = spark.read.parquet(f"{WAREHOUSE_PATH}/fact_watch_history")
    dim_date = spark.read.parquet(f"{WAREHOUSE_PATH}/dim_date")

    # Đăng ký thành các bảng ảo để viết truy vấn bằng cú pháp SQL thuần
    fact_watch.createOrReplaceTempView("fact_watch")
    dim_date.createOrReplaceTempView("dim_date")

    # Thực hiện câu lệnh SQL để tổng hợp 4 đặc trưng cốt lõi của mỗi người dùng
    query = """
        SELECT
            f.user_id,
            COUNT(f.session_id) * 1.0 AS total_views,
            SUM(f.watch_duration_minutes) * 1.0 AS total_duration_minutes,
            AVG(f.progress_percentage) AS avg_progress_percentage,
            SUM(CASE WHEN d.is_night = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS night_view_ratio
        FROM fact_watch f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY f.user_id
    """

    df = spark.sql(query).na.fill(0.0)
    logger.info(f"Tổng số lượng người dùng đủ điều kiện đưa vào phân cụm: {df.count()}")
    return df


# ============================================================
# 4. TÌM SỐ LƯỢNG CỤM TỐI ƯU (ELBOW / SILHOUETTE METHOD)
# ============================================================

def find_optimal_k(scaled_df, max_k=8):
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction",
        metricName="silhouette"
    )

    logger.info("========== TIẾN TRÌNH KHẢO SÁT CHỈ SỐ K TỐI ƯU ==========")
    
    for k in range(2, max_k + 1):
        model = KMeans(
            featuresCol="features",
            predictionCol="prediction",
            k=k,
            seed=44
        ).fit(scaled_df)

        prediction = model.transform(scaled_df)
        logger.info(
            f"Thử nghiệm K={k} | "
            f"Cost (Inertia)={model.summary.trainingCost:.2f} | "
            f"Silhouette Score={evaluator.evaluate(prediction):.4f}"
        )
# ============================================================
# 5. HUẤN LUYỆN KMEANS & GÁN NHÃN ĐỘNG (DYNAMIC LABELING)
# ============================================================

def build_and_run_kmeans(features_df):
    logger.info("-" * 60)
    logger.info("Đang gom nhóm các trường đặc trưng thành Vector...")
    feature_cols = ["total_views", "total_duration_minutes", "avg_progress_percentage", "night_view_ratio"]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    assembled = assembler.transform(features_df)

    logger.info("Đang tiến hành chuẩn hóa dữ liệu (StandardScaler)...")
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    scaled = scaler.fit(assembled).transform(assembled)

    # Chạy hàm khảo sát tìm K tối ưu
    find_optimal_k(scaled)

    logger.info("-" * 60)
    logger.info("Đang chạy thuật toán huấn luyện KMeans chính thức với K=3...")
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster_id",
        k=3,
        seed=42
    )
    model = kmeans.fit(scaled)
    predictions = model.transform(scaled)
    predictions.cache()

    # ghi log số lượng phần tử phân bổ cụm qua hàm bổ trợ
    logger.info("[ML Metrics] Báo cáo số lượng phần tử phân bổ trong mỗi mã cụm:")
    cluster_counts = predictions.groupBy("cluster_id").count().sort("cluster_id")
    logger.info(f"\n{get_show_string(cluster_counts, 5)}")

    # --------------------------------------------------------
    # TÍNH TÂM CỤM TRÊN DỮ LIỆU GỐC (GIẢI MÃ TÂM CỤM)
    # --------------------------------------------------------
    centers = (
        predictions
        .groupBy("cluster_id")
        .agg(
            F.round(F.avg("total_views"), 2).alias("total_views_center"),
            F.round(F.avg("total_duration_minutes"), 2).alias("duration_center"),
            F.round(F.avg("avg_progress_percentage"), 2).alias("progress_center"),
            F.round(F.avg("night_view_ratio"), 4).alias("night_ratio_center")
        )
        .sort("cluster_id")
    )
    centers.cache()

    logger.info("[ML Metrics] Tọa độ tâm cụm thực tế (Phục vụ phân tích hệ thống):")
    logger.info(f"\n{get_show_string(centers, 5)}")

    # Lưu thông tin tọa độ tâm cụm thực tế này xuống MongoDB làm metadata tham chiếu
    centers.write \
        .format("mongodb") \
        .option("collection", "ml_cluster_centroids") \
        .mode("overwrite") \
        .save()

    # --------------------------------------------------------
    # KIẾN TRÚC GÁN NHÃN ĐỘNG TẠI TẦNG SPARK
    # --------------------------------------------------------
    # Đưa tập tâm cụm rất nhỏ (3 dòng) về Driver dưới dạng mảng Python để xử lý logic gán nhãn chữ
    center_list = centers.collect()
    centers.unpersist()

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
    predictions.unpersist()
    return labeled


# ============================================================
# 6. XUẤT DỮ LIỆU SANG MONGODB (SERVING LAYER)
# ============================================================

def save_segments_to_mongodb(df):
    logger.info("-" * 60)
    logger.info("Đang xuất dữ liệu phân cụm hoàn chỉnh sang MongoDB...")

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
    final_df.cache()

    # In mẫu 5 hồ sơ khách hàng đã được phân cụm hoàn chỉnh vào log
    logger.info("[ML Metrics] Mẫu hồ sơ phân cụm người dùng thực tế chuẩn bị xuất xưởng:")
    logger.info(f"\n{get_show_string(final_df, 5)}")

    # Ghi dữ liệu xuống collection 'ml_user_segments' trong MongoDB
    final_df.write \
        .format("mongodb") \
        .option("collection", "ml_user_segments") \
        .mode("overwrite") \
        .save()

    logger.info("Đã xuất toàn bộ hồ sơ phân cụm người dùng xuống MongoDB")


# ============================================================
# 7. HÀM ĐIỀU PHỐI CHÍNH (MAIN FUNCTION)
# ============================================================
def main():
    spark = create_spark_session()

    try:
        logger.info("=" * 60)
        logger.info("===== KÍCH HOẠT PIPELINE MÁY HỌC PHÂN CỤM USER=====")
        logger.info("=" * 60)
        
        # Feature Engineering
        features = extract_user_features(spark)
        
        # Huấn luyện KMeans & Dán nhãn
        result = build_and_run_kmeans(features)
        
        # Xuất dữ liệu sang MongoDB
        save_segments_to_mongodb(result)
        
        logger.info("=" * 60)
        logger.info("===== KẾT THÚC PIPELINE MÁY HỌC =====")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline lỗi: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Đã đóng kết nối")

if __name__ == "__main__":
    main()