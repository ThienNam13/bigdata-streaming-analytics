from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, date_format, round, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.streaming import StreamingQueryListener
from logging.handlers import RotatingFileHandler
import logging
import sys
import os

# CẤU HÌNH LOGGING: Ghi song song ra Console và File cuốn chiếu trong thư mục dự án
LOG_DIR = "/opt/spark/logs"
# os.makedirs(LOG_DIR, exist_ok=True)
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except FileExistsError:
    # Nếu Docker báo lỗi "File exists" giả lập do cơ chế Mount, bỏ qua một cách an toàn
    pass
LOG_FILE_PATH = os.path.join(LOG_DIR, "spark_streaming.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        # Giới hạn dung lượng file log tối đa 20MB, lưu tối đa 5 file backup
        RotatingFileHandler(LOG_FILE_PATH, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("STREAMING_ENGINE")

# Bộ Listener giám sát hiệu năng Streaming tự động
class QueryMetricsListener(StreamingQueryListener):

    def onQueryStarted(self, event):
        logger.info("=" * 60)
        logger.info(f"[STREAM] Query Started")
        logger.info(f"[STREAM] Query ID: {event.id}")
        logger.info("=" * 60)

    def onQueryProgress(self, event):
        p = event.progress

        logger.info(
            f"""
================ STREAM METRICS =================
Batch ID            : {p.batchId}
Input Rows          : {p.numInputRows}
Input Rate          : {p.inputRowsPerSecond:.2f} rows/sec
Processing Rate     : {p.processedRowsPerSecond:.2f} rows/sec
Trigger Time        : {p.durationMs.get('triggerExecution', 0)} ms
=================================================
"""
        )

    def onQueryTerminated(self, event):
        logger.info(f"[STREAM TERMINATED] Luồng xử lý dữ liệu đã dừng.")

def main():
    logger.info("============================================================")
    logger.info("KHỞI ĐỘNG APACHE SPARK STRUCTURED STREAMING ENGINE...")
    logger.info("============================================================")

    # 1. Khởi tạo Spark Session cấu hình kết nối HDFS
    spark = SparkSession.builder \
        .appName("Video_Clickstream_Analytics_Engine") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(
        QueryMetricsListener()
    )
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
    logger.info("[DIMENSION] Đang nạp danh mục phim từ HDFS vào bộ nhớ...")
    try:
        static_movies = spark.read.parquet(f"{hdfs_namenode}/user/hadoop/warehouse/dim_movies") \
            .select("movie_id", "duration_minutes", "genre_primary")
    except Exception as e:
        logger.exception("STREAMING ENGINE FAILED")
        # đọc file CSV gốc từ HDFS nếu không thấy file parquet
        static_movies = spark.read.csv(f"{hdfs_namenode}/user/hadoop/movies.csv", header=True, inferSchema=True) \
            .select("movie_id", col("duration_minutes").cast("double"), "genre_primary")

    # Đưa bảng tĩnh vào cache để tối ưu hóa tốc độ Join ở các batch sau
    static_movies.cache()
    logger.info("Đang cache dim_movies...")
    logger.info(
        f"[DIMENSION] Đã nạp {static_movies.count()} bộ phim"
    )
    # ép kiểu dữ liệu thành chuỗi (String) rồi mới đẩy qua logger.info().
    def get_show_string(df, n=3):
        return df._jdf.showString(n, 20, False)
    
    # 4. Kết nối kafka để hứng luồng dữ liệu thô
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "video-clickstream") \
        .option("startingOffsets", "latest") \
        .load()

    raw_socket_stream = kafka_raw_stream.selectExpr("CAST(value AS STRING) as value")

    # 5. Hàm xử lý gộp từng Micro-Batch
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

        event_stats = (
            batch_df
            .groupBy("event_type")
            .count()
            .collect()
        )

        for row in event_stats:
            logger.info(
                f"[BATCH {batch_id}] {row['event_type']} = {row['count']} records"
            )

        # --- PHÂN NHÁNH 1: XỬ LÝ LOG XEM PHIM (WATCH) + STREAM-STATIC JOIN ---
        watch_batch = batch_df.filter(col("event_type") == "watch")

        # Thực hiện phép Join trực tiếp
        watch_joined = watch_batch.join(static_movies, on="movie_id", how="left")
        
        # Tính toán và ép trần tiến độ xem phim tối đa là 100.0%
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
        
        logger.info(
            f"[BATCH {batch_id}] Đã ghi dữ liệu WATCH xuống HDFS"
        )
            
        # 2. Ghi xuống MongoDB (Hot Data)
        watch_final.write \
            .format("mongodb") \
            .option("connection.uri", "mongodb://admin:secret@mongodb:27017") \
            .option("database", "streaming_analytics") \
            .option("collection", "fact_watch_history") \
            .mode("append") \
            .save()
        
        logger.info(
            f"[BATCH {batch_id}] Đã ghi dữ liệu WATCH xuống MongoDB"
        )
            
        # Ghi dữ liệu mẫu WATCH trực tiếp vào FILE LOG bằng stringify
        logger.info(f"[BATCH {batch_id}] Mẫu dữ liệu WATCH:\n{get_show_string(watch_final, 3)}")

        # --- PHÂN NHÁNH 2: XỬ LÝ LOG TÌM KIẾM (SEARCH) ---
        search_batch = batch_df \
            .filter(col("event_type") == "search") \
            .select(
                "search_id", "user_id", "search_query", "results_returned", 
                "clicked_result_position", "search_duration_seconds", 
                "had_typo", "device_type", "location_country", "date_key"
            )
 
        # 1. Ghi xuống HDFS (Cold Data)
        search_batch\
            .repartition(2, col("date_key")) \
            .write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("date_key") \
            .save(f"{hdfs_namenode}/user/hadoop/warehouse/fact_search_logs")
        
        logger.info(
            f"[BATCH {batch_id}] Đã ghi dữ liệu SEARCH xuống HDFS"
        )
            
        # 2. Ghi xuống MongoDB (Hot Data)
        search_batch.write \
            .format("mongodb") \
            .option("connection.uri", "mongodb://admin:secret@mongodb:27017") \
            .option("database", "streaming_analytics") \
            .option("collection", "fact_search_logs") \
            .mode("append") \
            .save()
        
        logger.info(
            f"[BATCH {batch_id}] Đã ghi dữ liệu SEARCH xuống MongoDB"
        )
            
        # Ghi dữ liệu mẫu SEARCH trực tiếp vào FILE LOG bằng stringify
        logger.info(f"[BATCH {batch_id}] Mẫu dữ liệu SEARCH:\n{get_show_string(search_batch, 3)}")

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
    
    # 7. KÍCH HOẠT LUỒNG STREAM ĐỔ VỀ HDFS và mongoDB / triggger cho 10s
    logger.info("ĐANG KÍCH HOẠT ĐƯỜNG ỐNG STREAMING ĐỔ VỀ STORAGE LAYER...")
    # LUỒNG 1: Ghi Log thô/đã làm sạch xuống HDFS và MongoDB
    query_main = refined_stream.writeStream \
        .foreachBatch(process_combined_batch) \
        .option("checkpointLocation", f"{hdfs_namenode}/user/hadoop/checkpoints/combined_stream") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Lọc riêng luồng xem phim (watch) để làm các bài toán thống kê thời gian thực
    watch_stream = refined_stream.filter(col("event_type") == "watch")

    # LUỒNG 2: Tính toán Hot Data cho Thể loại phim (Join)
    genre_joined_stream = watch_stream.join(static_movies, on="movie_id", how="left")

    # Tính toán Hot Data cho thể loại phim trực tiếp từ luồng sự kiện
    genre_counts = genre_joined_stream.groupBy("genre_primary").count()

    query_genre = genre_counts.writeStream \
        .format("mongodb") \
        .outputMode("complete") \
        .option("connection.uri", "mongodb://admin:secret@mongodb:27017") \
        .option("database", "streaming_analytics") \
        .option("collection", "report_genre_popularity_stream") \
        .option("checkpointLocation", f"{hdfs_namenode}/user/hadoop/checkpoints/genre_hot") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # LUỒNG 3: Tính toán Hot Data cho Cơ cấu Thiết bị
    device_counts = watch_stream.groupBy("device_type").count()

    query_device = device_counts.writeStream \
        .format("mongodb") \
        .outputMode("complete") \
        .option("connection.uri", "mongodb://admin:secret@mongodb:27017") \
        .option("database", "streaming_analytics") \
        .option("collection", "report_top_devices_stream") \
        .option("checkpointLocation", f"{hdfs_namenode}/user/hadoop/checkpoints/device_hot") \
        .trigger(processingTime="10 seconds") \
        .start()

    # Giữ cá luồng chạy song song không bị ngắt quãng
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()