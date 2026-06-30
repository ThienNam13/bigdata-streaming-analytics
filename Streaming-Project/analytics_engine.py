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
import logging
import sys

# ============================================================
# LOGGING & CONFIG
# ============================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ANALYTICS")

HDFS_ROOT = "hdfs://namenode:9000"
WAREHOUSE_PATH = f"{HDFS_ROOT}/user/hadoop/warehouse"

# MongoDB Config
MONGO_URI = "mongodb://admin:secret@mongodb:27017"
DATABASE = "streaming_analytics"

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
    """Bài toán 0: Phân tích thiết bị phổ biến"""
    logger.info("Task 0: Analyzing Top Devices...")
    
    query = """
        SELECT 
            f.device_type,
            COUNT(f.session_id) AS total_views
        FROM fact_watch f
        GROUP BY f.device_type
        ORDER BY total_views DESC
    """
    result = spark.sql(query)
    result.write.format("mongodb").option("collection", "report_top_devices").mode("overwrite").save()
    logger.info("Task 0 completed.")

def run_genre_popularity_analysis(spark):
    """Bài toán 1: Genre Popularity"""
    logger.info("Task 1: Analyzing Genre Popularity...")
    
    query = """
        SELECT 
            m.genre_primary,
            COUNT(f.session_id) AS watch_count,
            AVG(m.imdb_rating) AS avg_rating,
            AVG(f.watch_duration_minutes) AS avg_watch_minutes
        FROM fact_watch f
        JOIN dim_movies m ON f.movie_id = m.movie_id
        GROUP BY m.genre_primary
        ORDER BY watch_count DESC
    """
    result = spark.sql(query)
    result.write.format("mongodb").option("collection", "report_genre_popularity").mode("overwrite").save()
    logger.info("Task 1 completed.")

def run_peak_hours_analysis(spark):
    """Bài toán 2: Tìm khung giờ vàng (Peak Hours)"""
    logger.info("Task 2: Analyzing Peak Hours...")
    
    query = """
        SELECT d.hour, COUNT(DISTINCT f.user_id) AS active_users,
            AVG(f.progress_percentage) AS avg_progress,
            AVG(f.watch_duration_minutes) AS avg_watch_duration
        FROM fact_watch f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.hour
        ORDER BY d.hour ASC
    """
    result = spark.sql(query)
    # Ghi vào MongoDB collection: report_peak_hours
    result.write.format("mongodb").option("collection", "report_peak_hours").mode("overwrite").save()
    logger.info("Task 2 completed and saved to MongoDB.")

def run_binge_watching_analysis(spark):
    """Bài toán 3: Phân tích thể loại phim được 'cày' đêm muộn"""
    logger.info("Task 3: Analyzing Night Binge-watching habits...")
    
    query = """
        SELECT 
            m.genre_primary,
            SUM(f.watch_duration_minutes) AS total_watch_duration,
            AVG(f.progress_percentage) AS avg_progress
        FROM fact_watch f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_movies m ON f.movie_id = m.movie_id
        WHERE d.is_night = true
        GROUP BY m.genre_primary
        ORDER BY total_watch_duration DESC
    """
    result = spark.sql(query)
    result.write.format("mongodb").option("collection", "report_binge_genres").mode("overwrite").save()
    logger.info("Task 3 completed.")

def run_churn_risk_analysis(spark):
    """Bài toán 4: Rủi ro rời bỏ do trải nghiệm Buffering tệ (Nghẽn mạng)"""
    logger.info("Task 4: Analyzing Buffering & Churn Risk...")
    
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
    result.write.format("mongodb").option("collection", "report_churn_risk").mode("overwrite").save()
    logger.info("Task 4 completed.")

def run_search_failure_analysis(spark):
    """Bài toán 5: Phân tích các từ khóa tìm kiếm không có kết quả"""
    logger.info("Task 5: Analyzing Search Failures...")
    
    query = """
        SELECT 
            f.search_query,
            f.had_typo,
            COUNT(f.search_id) AS failed_count
        FROM fact_search f
        WHERE f.results_returned = 0
        GROUP BY f.search_query, f.had_typo
        ORDER BY failed_count DESC
        LIMIT 20
    """
    result = spark.sql(query)
    result.write.format("mongodb").option("collection", "report_failed_searches").mode("overwrite").save()
    logger.info("Task 5 completed.")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    logger.info("=== STARTING BI ANALYTICS ENGINE ===")
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

        logger.info("🏆 ALL ANALYTICS TASKS COMPLETED SUCCESSFULLY!")

    except Exception as e:
        logger.error(f"❌ ANALYTICS ENGINE FAILED: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()