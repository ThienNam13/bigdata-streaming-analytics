from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, date_format, round, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import logging
import sys
import os

def main():
    logger.info("============================================================")
    logger.info("KHỞI TẠO APACHE SPARK STRUCTURED STREAMING ENGINE...")
    logger.info("============================================================")

    # 1. Khởi tạo Spark Session cấu hình kết nối HDFS
    spark = SparkSession.builder \
        .appName("Video_Clickstream_Analytics_Engine") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # 2. Định nghĩa Schema tổng hợp để giải mã từ Kafka
    combined_schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("current_timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("movie_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("watch_duration_minutes", DoubleType(), True),
        StructField("progress_percentage", DoubleType(), True),
        StructField("action", StringType(), True),
        StructField("quality", StringType(), True),
        StructField("user_rating", IntegerType(), True),
        StructField("search_id", StringType(), True),
        StructField("search_query", StringType(), True),
        StructField("results_returned", IntegerType(), True),
        StructField("clicked_result_position", IntegerType(), True),
        StructField("search_duration_seconds", DoubleType(), True),
        StructField("had_typo", StringType(), True),
        StructField("location_country", StringType(), True)
    ])

    hdfs_namenode = "hdfs://namenode:9000"

    # 🌟 3. NẠP BẢNG TĨNH MOVIES (STATIC DATAFRAME) ĐỂ PHỤC VỤ STREAM-STATIC JOIN
    # Đọc dữ liệu từ thư mục bảng Dim Movies đã chuẩn hóa trên HDFS
    logger.info("[STATIC DATA] Đang nạp danh mục phim từ HDFS vào bộ nhớ...")
    try:
        static_movies = spark.read.parquet(f"{hdfs_namenode}/user/hadoop/warehouse/dim_movies") \
            .select("movie_id", "duration_minutes")
    except Exception as e:
        logger.exception("STREAMING ENGINE FAILED")
        # Phương án dự phòng nếu bạn chưa chuyển CSV thành Parquet, đọc thẳng file CSV gốc từ HDFS
        static_movies = spark.read.csv(f"{hdfs_namenode}/user/hadoop/movies.csv", header=True, inferSchema=True) \
            .select("movie_id", col("duration_minutes").cast("double"))

    # Đưa bảng tĩnh vào cache để tối ưu hóa tốc độ Join ở các batch sau
    static_movies.cache()
    logger.info("Đang cache dim_movies vào RAM...")
    logger.info(f"Số lượng bản ghi dim_movies: {static_movies.count()}")
    # 4. Kết nối kafka để hứng luồng dữ liệu thô
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "video-clickstream") \
        .option("startingOffsets", "latest") \
        .load()

    raw_socket_stream = kafka_raw_stream.selectExpr("CAST(value AS STRING) as value")

    # 5. Hàm xử lý gộp từng Micro-Batch
    # 5. Hàm xử lý gộp từng Micro-Batch (Đã tối ưu hóa hiệu năng & Logic)
    def process_combined_batch(batch_df, batch_id):
        
        # Parse timestamp từ trường gốc trong log
        batch_df = batch_df.withColumn(
            "event_ts",
            to_timestamp(col("current_timestamp"), "yyyy-MM-dd HH:mm:ss")
        ).withColumn(
            "date_key",
            date_format(col("event_ts"), "yyyyMMddHH")
        )

        logger.info(
            f"=== [BATCH {batch_id}] ĐANG XỬ LÝ VI-LUỒNG THỜI GIAN THỰC ==="
        )
        batch_df.cache() # Cache một lần duy nhất cho toàn bộ luồng xử lý bên dưới

        # --- PHÂN NHÁNH 1: XỬ LÝ LOG XEM PHIM (WATCH) + STREAM-STATIC JOIN ---
        watch_batch = batch_df.filter(col("event_type") == "watch")
        watch_count = watch_batch.count()
        logger.info(
            f"[BATCH {batch_id}] WATCH RECORDS = {watch_count}"
        )

        # Thực hiện phép Join trực tiếp (Nếu watch_batch trống, Spark tự động skip rất nhanh mà không lỗi)
        watch_joined = watch_batch.join(static_movies, on="movie_id", how="left")
        
        # 🌟 SỬA LOGIC: Tính toán và ép trần tiến độ xem phim tối đa là 100.0%
        calculated_progress = when(col("duration_minutes") > 0, 
                                   round((col("watch_duration_minutes") / col("duration_minutes")) * 100, 2)) \
                              .otherwise(0.0)
                              
        watch_final = watch_joined.withColumn(
            "progress_percentage",
            when(calculated_progress > 100.0, 100.0).otherwise(calculated_progress)
        ).select(
            "session_id", "user_id", "movie_id", "device_type", 
            "watch_duration_minutes", "progress_percentage", 
            "action", "quality", "user_rating", "location_country", "date_key"
        )

        # 1. Ghi xuống HDFS (Cold Data)  ###watch_final.write \
        watch_final \
            .repartition(2, col("date_key")) \
            .write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("date_key") \
            .save(f"{hdfs_namenode}/user/hadoop/warehouse/fact_watch_history")
            
        # 2. Ghi xuống MongoDB (Hot Data)
        watch_final.write \
            .format("mongodb") \
            .option("connection.uri", "mongodb://admin:secret@mongodb:27017") \
            .option("database", "streaming_analytics") \
            .option("collection", "fact_watch_history") \
            .mode("append") \
            .save()
            
        # Dùng .show() để kiểm tra nhanh trên màn hình console thay vì dùng .count()
        watch_final.show(3, truncate=False)

        # --- PHÂN NHÁNH 2: XỬ LÝ LOG TÌM KIẾM (SEARCH) ---
        search_batch = batch_df \
            .filter(col("event_type") == "search") \
            .select(
                "search_id", "user_id", "search_query", "results_returned", 
                "clicked_result_position", "search_duration_seconds", 
                "had_typo", "device_type", "location_country", "date_key"
            )
        search_count = search_batch.count()

        logger.info(
            f"[BATCH {batch_id}] SEARCH RECORDS = {search_count}"
        )

        # 1. Ghi xuống HDFS (Cold Data)
        search_batch\
            .repartition(2, col("date_key")) \
            .write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("date_key") \
            .save(f"{hdfs_namenode}/user/hadoop/warehouse/fact_search_logs")
            
        # 2. Ghi xuống MongoDB (Hot Data)
        search_batch.write \
            .format("mongodb") \
            .option("connection.uri", "mongodb://admin:secret@mongodb:27017") \
            .option("database", "streaming_analytics") \
            .option("collection", "fact_search_logs") \
            .mode("append") \
            .save()
            
        search_batch.show(3, truncate=False)

        # Giải phóng bộ nhớ RAM sau khi kết thúc Micro-batch
        batch_df.unpersist()
    
    # 6. Phân tách Json và định hình mốc thời gian
    parsed_stream = raw_socket_stream \
        .select(from_json(col("value"), combined_schema).alias("data")) \
        .select("data.*")
    
    refined_stream = parsed_stream \
        .filter(col("user_id").isNotNull()) \
        .withColumn("ts", to_timestamp(col("current_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("date_key", date_format(col("ts"), "yyyyMMddHH"))

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    
    # 7. KÍCH HOẠT LUỒNG STREAM DUY NHẤT ĐỔ VỀ HDFS
    logger.info("ĐANG KÍCH HOẠT ĐƯỜNG ỐNG STREAMING ĐỔ VỀ HDFS...")
    query = refined_stream.writeStream \
        .foreachBatch(process_combined_batch) \
        .option("checkpointLocation", f"{hdfs_namenode}/user/hadoop/checkpoints/combined_stream") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/opt/logs/spark_streaming.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("STREAMING_ENGINE")
if __name__ == "__main__":
    main()