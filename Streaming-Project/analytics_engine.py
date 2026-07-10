# ============================================================
# FILE: analytics_engine.py
# TẦNG 6 - BI ANALYTICS ENGINE
#
# Nhiệm vụ
# --------
# 1. Đọc Star Schema (Parquet) từ HDFS
# 2. Thực hiện các truy vấn Spark SQL nghiệp vụ
# 3. Tính toán Metric: Peak Hours, Binge Watching, Churn Risk
# 4. Ghi kết quả sang MongoDB (Serving Layer)
#
# Project:
# USER BEHAVIOR ANALYTICS FOR VIDEO STREAMING PLATFORM
# ============================================================

from pyspark.sql import SparkSession
from logging.handlers import RotatingFileHandler
import logging
import sys
import os
# ============================================================
# LOGGING
# ============================================================
LOG_DIR = "/opt/spark/logs"
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except FileExistsError:
    # Nếu Docker báo lỗi "File exists" giả lập do cơ chế Mount, bỏ qua một cách an toàn
    pass

LOG_FILE_PATH = os.path.join(LOG_DIR, "analytics.log")
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE_PATH, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"),        # In log ra console
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ANALYTICS")

HDFS_ROOT = "hdfs://namenode:9000"
WAREHOUSE_PATH = f"{HDFS_ROOT}/user/hadoop/warehouse"

# MongoDB Config
MONGO_URI = "mongodb://admin:secret@mongodb:27017"
DATABASE = "streaming_analytics"
# ============================================================
# UTILS: HÀM TRÍCH XUẤT BẢNG SPARK THÀNH CHUỖI ĐỂ GHI LOG
# ============================================================
def get_show_string(df, n=5):
    """Chuyển đổi n dòng của DataFrame thành chuỗi ký tự dạng bảng sạch để ghi vào log file"""
    try:
        return df._jdf.showString(n, 20, False)
    except Exception:
        return "Không thể hiển thị mẫu dữ liệu."
    
# ============================================================
# CREATE SPARK SESSION (With MongoDB Connector)
# ============================================================

def create_spark():
    logger.info("Creating Spark Session for Analytics...")
    spark = (
        SparkSession.builder
        .appName("VideoStreaming-AnalyticsEngine")
        # Cấu hình để ghi dữ liệu xuống MongoDB
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.database", DATABASE)
        .config("spark.sql.adaptive.enabled","true")
        .config("spark.sql.shuffle.partitions","4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ============================================================
# DATA LOADING & VIEW REGISTRATION
# ============================================================

def register_warehouse_views(spark):
    """Nạp Star Schema từ HDFS và đăng ký SQL Views"""
    logger.info("Loading Star Schema from HDFS...")
    
    tables = {
        "dim_users": f"{WAREHOUSE_PATH}/dim_users",
        "dim_movies": f"{WAREHOUSE_PATH}/dim_movies",
        "dim_date": f"{WAREHOUSE_PATH}/dim_date",
        "fact_watch": f"{WAREHOUSE_PATH}/fact_watch_history",
        "fact_search": f"{WAREHOUSE_PATH}/fact_search_logs"
    }

    for view_name, path in tables.items():
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(view_name)
        logger.info(f"{view_name} registered.")

# ============================================================
# ANALYTICS TASKS
# ============================================================
def run_top_devices_analysis(spark):
    """1: Phân tích thiết bị phổ biến"""
    logger.info(" Analyzing Top Devices...")
    
    query = """
        SELECT 
            f.device_type,
            COUNT(f.session_id) AS total_views
        FROM fact_watch f
        GROUP BY f.device_type
        ORDER BY total_views DESC
    """
    result = spark.sql(query)
    result.cache()
    
    # In số liệu ra log
    logger.info(f"[Metrics 1] Tổng số loại thiết bị ghi nhận: {result.count()}")
    logger.info(f"\n{get_show_string(result, 5)}")

    result.write.format("mongodb").option("collection", "report_top_devices").mode("overwrite").save()
    result.unpersist()
    logger.info("Lưu vào MongoDB collection 'report_top_devices'.")

def run_genre_popularity_analysis(spark):
    """2: Genre Popularity"""
    logger.info(" Analyzing Genre Popularity...")
    
    query = """
        SELECT 
            m.genre_primary,
            COUNT(f.session_id) AS watch_count,
            ROUND(AVG(m.imdb_rating), 2) AS avg_imdb_rating,
            ROUND(AVG(f.watch_duration_minutes), 2) AS avg_watch_minutes
        FROM fact_watch f
        JOIN dim_movies m ON f.movie_id = m.movie_id
        GROUP BY m.genre_primary
        ORDER BY watch_count DESC
    """
    result = spark.sql(query)
    result.cache()
    
    logger.info(f"[Metrics 2] Số lượng thể loại phim đã phân tích: {result.count()}")
    logger.info(f"\n{get_show_string(result, 5)}")
    result.write.format("mongodb").option("collection", "report_genre_popularity").mode("overwrite").save()
    result.unpersist()
    logger.info("Lưu vào MongoDB collection 'report_genre_popularity'.")

def run_peak_hours_analysis(spark):
    """3: Tìm khung giờ vàng (Peak Hours)"""
    logger.info(" Analyzing Peak Hours...")
    
    query = """
        SELECT 
            d.hour, 
            COUNT(DISTINCT f.user_id) AS active_users,
            ROUND(AVG(f.progress_percentage), 2) AS avg_progress_pct,
            ROUND(AVG(f.watch_duration_minutes), 2) AS avg_watch_duration
        FROM fact_watch f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.hour
        ORDER BY active_users DESC
    """
    result = spark.sql(query)
    result.cache()
    
    logger.info(f"[Metrics 3] Top các khung giờ có lượng truy cập cao nhất:")
    logger.info(f"\n{get_show_string(result, 5)}")

    # Ghi vào MongoDB collection: report_peak_hours
    result.write.format("mongodb").option("collection", "report_peak_hours").mode("overwrite").save()
    result.unpersist()
    logger.info("Lưu vào MongoDB collection 'report_peak_hours'.")

def run_binge_watching_analysis(spark):
    """4. Phân tích thể loại phim được 'cày' đêm muộn"""
    logger.info(" Analyzing Night Binge-watching habits...")
    
    query = """
        SELECT 
            m.genre_primary,
            ROUND(SUM(f.watch_duration_minutes), 2) AS total_watch_duration,
            ROUND(AVG(f.progress_percentage), 2) AS avg_progress_pct
        FROM fact_watch f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_movies m ON f.movie_id = m.movie_id
        WHERE d.is_night = true
        GROUP BY m.genre_primary
        ORDER BY total_watch_duration DESC
    """
    result = spark.sql(query)
    result.cache()
    
    logger.info(f"[Metrics 4] Xu hướng xem phim đêm muộn theo Thể loại:")
    logger.info(f"\n{get_show_string(result, 5)}")

    result.write.format("mongodb").option("collection", "report_binge_genres").mode("overwrite").save()
    result.unpersist()
    logger.info("Lưu vào MongoDB collection 'report_binge_genres'.")

def run_churn_risk_analysis(spark):
    """5: Rủi ro rời bỏ do trải nghiệm tắt ứng dụng sớm (Buffering/chán)"""
    logger.info("Analyzing Buffering & Churn Risk...")
    
    query = """
        SELECT 
            f.device_type,
            f.location_country,
            f.quality,
            COUNT(f.session_id) AS drop_events,
            AVG(f.progress_percentage) AS avg_progress_at_drop,
            AVG(f.watch_duration_minutes) AS avg_watch_duration
        FROM fact_watch f
        WHERE f.progress_percentage < 10.0 
          AND f.action = 'paused'
        GROUP BY f.device_type, f.location_country, f.quality
        ORDER BY drop_events DESC
    """
    result = spark.sql(query)
    result.cache()
    
    logger.info(f"[Metrics 5] Các nhóm người dùng có tỷ lệ thoát sớm (Rủi ro Churn) cao nhất:")
    logger.info(f"\n{get_show_string(result, 5)}")

    result.write.format("mongodb").option("collection", "report_churn_risk").mode("overwrite").save()
    result.unpersist()
    logger.info("Lưu vào MongoDB collection 'report_churn_risk'.")

def run_search_failure_analysis(spark):
    """6: Phân tích các từ khóa tìm kiếm không có kết quả"""
    logger.info(" Analyzing Search Failures...")
    
    query = """
        SELECT 
            f.search_query,
            f.had_typo,
            COUNT(f.search_id) AS failed_count
        FROM fact_search f
        WHERE f.results_returned = 0
        GROUP BY f.search_query, f.had_typo
        ORDER BY failed_count DESC
        LIMIT 10
    """
    result = spark.sql(query)
    result.cache()
    
    logger.info(f"[Task 5 Metrics] Top 10 từ khóa tìm kiếm thất bại nhiều nhất:")
    logger.info(f"\n{get_show_string(result, 10)}")

    result.write.format("mongodb").option("collection", "report_failed_searches").mode("overwrite").save()
    result.unpersist()
    logger.info("Lưu vào MongoDB collection 'report_failed_searches'.")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    logger.info("=== KÍCH HOẠT BI ANALYTICS ENGINE ===")
    spark = create_spark()

    try:
        # 1. Chuẩn bị dữ liệu
        register_warehouse_views(spark)

        # 2. Chạy các tiến trình phân tích
        run_top_devices_analysis(spark)
        run_genre_popularity_analysis(spark)
        run_peak_hours_analysis(spark)
        run_binge_watching_analysis(spark)
        run_churn_risk_analysis(spark)
        run_search_failure_analysis(spark)

        logger.info("HOÀN THÀNH PHÂN TÍCH")

    except Exception as e:
        logger.error(f"PHÂN TÍCH THẤT BẠI: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()