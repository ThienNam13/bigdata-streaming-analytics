# ============================================================
# FILE: warehouse_builder.py
# TẦNG 6 - DATA WAREHOUSE BUILDER
#
# Nhiệm vụ
# --------
# 1. Đọc dữ liệu Dimension từ CSV
# 2. Đọc Fact Tables từ HDFS
# 3. Sinh Star Schema
# 4. Lưu Parquet về HDFS
# 5. Đăng ký Spark SQL Views
# 6. Kiểm tra Referential Integrity
#
# Project:
# USER BEHAVIOR ANALYTICS FOR VIDEO STREAMING PLATFORM
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, year, month, dayofmonth, hour,
    dayofweek, date_format, to_timestamp, concat_ws, desc
)
from pyspark.sql.types import *
from pyspark.sql.functions import to_date
import logging
import sys

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("WAREHOUSE")

# ============================================================
# HDFS CONFIGURATION
# ============================================================

HDFS_ROOT = "hdfs://namenode:9000"
WAREHOUSE_PATH = f"{HDFS_ROOT}/user/hadoop/warehouse"

FACT_WATCH_PATH = f"{WAREHOUSE_PATH}/fact_watch_history"
FACT_SEARCH_PATH = f"{WAREHOUSE_PATH}/fact_search_logs"

DIM_USERS_PATH = f"{WAREHOUSE_PATH}/dim_users"
DIM_MOVIES_PATH = f"{WAREHOUSE_PATH}/dim_movies"
DIM_DATE_PATH = f"{WAREHOUSE_PATH}/dim_date"

# ============================================================
# LOCAL SOURCE FILES
# ============================================================

USERS_CSV = "/opt/spark/users.csv"
MOVIES_CSV = "/opt/spark/movies.csv"

# ============================================================
# CREATE SPARK SESSION
# ============================================================

def create_spark():
    logger.info("Creating Spark Session...")
    spark = (
        SparkSession.builder
        .appName("VideoStreaming-DataWarehouseBuilder")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.warehouse.dir", f"{WAREHOUSE_PATH}/spark_catalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session created successfully.")
    return spark

# ============================================================
# AGE GROUP
# ============================================================

def build_age_group(df):
    logger.info("Building age_group...")
    return (
        df.withColumn(
            "age_group",
            when(col("age") < 18, "Children")
            .when((col("age") >= 18) & (col("age") <= 35), "Young Adults")
            .when((col("age") >= 36) & (col("age") <= 55), "Middle-aged")
            .otherwise("Senior")
        )
    )

# ============================================================
# READ USERS DIMENSION
# ============================================================

def load_dim_users(spark):
    logger.info("Loading users.csv ...")
    users = (
        spark.read.option("header", True).option("inferSchema", True).csv(USERS_CSV)
    )
    users = build_age_group(users)
    # Loại bỏ duplicate user_id
    users = users.dropDuplicates(["user_id"])
    users = users.select(
        "user_id", "age", "age_group", "gender", "country",
        "state_province", "city", "subscription_plan",
        "is_active", "monthly_spend", "primary_device"
    )
    logger.info(f"Users loaded : {users.count()}")
    return users

# ============================================================
# READ MOVIES DIMENSION
# ============================================================

def load_dim_movies(spark):
    logger.info("Loading movies.csv ...")
    movies = (
        spark.read.option("header", True).option("inferSchema", True).csv(MOVIES_CSV)
    )
    # Loại bỏ duplicate movie_id
    movies = movies.dropDuplicates(["movie_id"])
    movies = movies.select(
        "movie_id", "title", "content_type", "genre_primary",
        "genre_secondary", "release_year", "duration_minutes",
        "imdb_rating", "is_netflix_original"
    )
    logger.info(f"Movies loaded : {movies.count()}")
    return movies

# ============================================================
# LOAD FACT WATCH
# ============================================================

def load_fact_watch(spark):
    logger.info("Loading fact_watch_history ...")
    watch = spark.read.parquet(FACT_WATCH_PATH)
    logger.info(f"Watch records : {watch.count()}")
    return watch

# ============================================================
# LOAD FACT SEARCH
# ============================================================

def load_fact_search(spark):
    logger.info("Loading fact_search_logs ...")
    search = spark.read.parquet(FACT_SEARCH_PATH)
    logger.info(f"Search records : {search.count()}")
    return search

# ============================================================
# DATA QUALITY CHECK
# ============================================================

def check_null_key(df, key_name):
    total_null = df.filter(col(key_name).isNull()).count()
    if total_null > 0:
        logger.warning(f"{key_name} contains {total_null} NULL values.")
    else:
        logger.info(f"{key_name} validated.")

def check_duplicate(df, key_name):
    duplicate = (
        df.groupBy(key_name).count().filter(col("count") > 1).count()
    )
    if duplicate > 0:
        logger.warning(f"{duplicate} duplicated keys detected in {key_name}")
    else:
        logger.info(f"{key_name} has no duplicates.")

# ============================================================
# VALIDATE DIMENSIONS
# ============================================================

def validate_dimension(df, key):
    logger.info(f"Checking Dimension [{key}]")
    check_null_key(df, key)
    check_duplicate(df, key)
    logger.info("Validation completed.")

# ============================================================
# PREVIEW DATA
# ============================================================

def preview(df, title):
    logger.info("=" * 70)
    logger.info(title)
    logger.info("=" * 70)
    df.show(5, truncate=False)
    logger.info(f"Rows : {df.count()}")
    logger.info("=" * 70)

# ============================================================
# BUILD DYNAMIC DATE DIMENSION
# ============================================================

def build_dim_date(spark, watch_df, search_df):
    """
    Sinh bảng chiều dim_date động dựa trên date_key từ 2 bảng Fact
    Giả định date_key có định dạng chuỗi: 'yyyyMMddHH'
    """
    logger.info("Building dim_date from Fact tables dynamically...")

    # Thu thập tất cả các date_key độc nhất từ cả lịch sử xem phim và tìm kiếm
    watch_keys = watch_df.select("date_key")
    search_keys = search_df.select("date_key")
    distinct_keys = watch_keys.union(search_keys).distinct()

    # Áp dụng các hàm thời gian được Import để bóc tách thuộc tính
    df_with_ts = (
        distinct_keys
        .withColumn("date_key", col("date_key").cast("string"))
        .withColumn(
            "ts",
            to_timestamp(col("date_key"), "yyyyMMddHH")
        )
    )

    dim_date = df_with_ts.select(
        col("date_key"),
        col("ts").alias("full_timestamp"),
        hour(col("ts")).alias("hour"),
        dayofmonth(col("ts")).alias("day"),
        month(col("ts")).alias("month"),
        year(col("ts")).alias("year"),
        dayofweek(col("ts")).alias("day_of_week"),
        date_format(col("ts"), "E").alias("day_name"),
        # Xác định cuối tuần (1: Chủ Nhật, 7: Thứ Bảy trong PySpark)
        when(dayofweek(col("ts")).isin(1, 7), True)
        .otherwise(False)
        .alias("is_weekend"),
        # Xác định khung giờ đêm (Từ 22h đêm đến 4h sáng hôm sau)
        when((hour(col("ts")) >= 22) | (hour(col("ts")) <= 4), True)
        .otherwise(False)
        .alias("is_night")
    )

    logger.info(f"Dynamic dim_date built with {dim_date.count()} time keys.")
    return dim_date


# ============================================================
# REFERENTIAL INTEGRITY (RI) CHECK
# ============================================================

def check_referential_integrity(fact_df, dim_df, fact_fk, dim_pk, fact_name, dim_name):
    """
    Sử dụng Left Anti Join để phát hiện các bản ghi trong Fact 
    chứa Foreign Key không tồn tại ở bảng Dimension tương ứng.
    """
    logger.info(f"Verifying RI: {fact_name}.{fact_fk} -> {dim_name}.{dim_pk}")

    # Left Anti Join trả về những dòng thuộc bảng bên trái KHÔNG khớp với bảng bên phải
    orphans = fact_df.join(
        dim_df, 
        fact_df[fact_fk] == dim_df[dim_pk], 
        "left_anti"
    )
    
    orphan_count = orphans.count()

    if orphan_count > 0:
        logger.warning(
            f"RI VIOLATION: Detected {orphan_count} orphan records in {fact_name} "
            f"where {fact_fk} does not exist in {dim_name}!"
        )
        # In ra 5 dòng lỗi tiêu biểu để Data Engineer debug
        orphans.show(5, truncate=False)
    else:
        logger.info(f"RI SUCCESS: All keys in {fact_name}.{fact_fk} are valid.")


# ============================================================
# WRITE TO HDFS WAREHOUSE
# ============================================================

def save_dimension_to_hdfs(df, path, dim_name):
    logger.info(f"Writing Dimension [{dim_name}] to HDFS Parquet...")
    (
        df.write
        .mode("overwrite")
        .parquet(path)
    )
    logger.info(f"Dimension [{dim_name}] saved successfully at {path}")


# ============================================================
# MAIN ORCHESTRATION PIPELINE
# ============================================================

def main():
    logger.info("=== STARTING DATA WAREHOUSE BUILDER PIPELINE ===")

    # 1. Khởi tạo Spark Session
    spark = create_spark()

    try:
        # 2. Nạp và làm sạch các bảng Dimension từ file CSV cục bộ
        dim_users = load_dim_users(spark)
        dim_movies = load_dim_movies(spark)

        # 3. Nạp dữ liệu Fact Tables hiện tại từ HDFS
        fact_watch = load_fact_watch(spark)
        fact_search = load_fact_search(spark)

        # 4. Kiểm tra Chất lượng dữ liệu nội tại của bảng Dim (Null & Duplicate)
        validate_dimension(dim_users, "user_id")
        validate_dimension(dim_movies, "movie_id")

        # 5. Sinh động bảng chiều thời gian dim_date từ thực tế dữ liệu
        dim_date = build_dim_date(spark, fact_watch, fact_search)
        validate_dimension(dim_date, "date_key")

        # 6. Kiểm tra Toàn vẹn Tham chiếu (Referential Integrity) giữa các tầng
        logger.info("--- Performing Cross-Table Referential Integrity Checks ---")
        check_referential_integrity(fact_watch, dim_users, "user_id", "user_id", "fact_watch_history", "dim_users")
        check_referential_integrity(fact_watch, dim_movies, "movie_id", "movie_id", "fact_watch_history", "dim_movies")
        check_referential_integrity(fact_watch, dim_date, "date_key", "date_key", "fact_watch_history", "dim_date")
        
        check_referential_integrity(fact_search, dim_users, "user_id", "user_id", "fact_search_logs", "dim_users")
        check_referential_integrity(fact_search, dim_date, "date_key", "date_key", "fact_search_logs", "dim_date")

        # 7. Lưu trữ các bảng Dimension sạch xuống HDFS Warehouse dạng Parquet
        save_dimension_to_hdfs(dim_users, DIM_USERS_PATH, "dim_users")
        save_dimension_to_hdfs(dim_movies, DIM_MOVIES_PATH, "dim_movies")
        save_dimension_to_hdfs(dim_date, DIM_DATE_PATH, "dim_date")
        verify_users = spark.read.parquet(DIM_USERS_PATH)

        logger.info(
            f"Verify dim_users : {verify_users.count()}"
        )

        # 8. Đăng ký Spark SQL Views (Tạo nền tảng cho analytics_engine.py truy vấn)
        logger.info("Registering Global Spark SQL Temp Views...")
        dim_users.createOrReplaceGlobalTempView("dim_users")
        dim_movies.createOrReplaceGlobalTempView("dim_movies")
        dim_date.createOrReplaceGlobalTempView("dim_date")
        fact_watch.createOrReplaceGlobalTempView("fact_watch")
        fact_search.createOrReplaceGlobalTempView("fact_search")

        # 9. Preview kết quả cuối cùng trước khi đóng Session
        preview(dim_users, "PREVIEW: DIM_USERS (HDFS STORED)")
        preview(dim_movies, "PREVIEW: DIM_MOVIES (HDFS STORED)")
        preview(dim_date, "PREVIEW: DIM_DATE (DYNAMICALLY GENERATED)")

        logger.info("Star Schema is fully established.")
        logger.info(f"dim_users       : {dim_users.count()}")
        logger.info(f"dim_movies      : {dim_movies.count()}")
        logger.info(f"dim_date        : {dim_date.count()}")
        logger.info(f"fact_watch      : {fact_watch.count()}")
        logger.info(f"fact_search     : {fact_search.count()}")
        logger.info("="*70)

    except Exception as e:
        logger.error(f"❌ PIPELINE FAILED with error: {str(e)}")
        sys.exit(1)

    finally:
        logger.info("Stopping Spark Session...")
        spark.stop()


if __name__ == "__main__":
    main()