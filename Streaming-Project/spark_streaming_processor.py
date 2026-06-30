from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    print("============================================================")
    print("KHỞI TẠO APACHE SPARK STRUCTURED STREAMING ENGINE...")
    print("============================================================")

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

    # 3. Kết nối kafka để hứng luồng dữ liệu thô
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "video-clickstream") \
        .option("startingOffsets", "latest") \
        .load()

    raw_socket_stream = kafka_raw_stream.selectExpr("CAST(value AS STRING) as value")
    hdfs_namenode = "hdfs://namenode:9000"

    # 4. Hàm xử lý gộp từng Micro-Batch (Đã sửa lỗi cấu trúc)
    def process_combined_batch(batch_df, batch_id):
        # Parse timestamp từ trường gốc trong log
        batch_df = batch_df.withColumn(
            "event_ts",
            to_timestamp(col("current_timestamp"), "yyyy-MM-dd HH:mm:ss")
        )

        # Sinh date_key từ event_ts thay vì current_timestamp()
        batch_df = batch_df.withColumn(
            "date_key",
            date_format(col("event_ts"), "yyyyMMddHH")
        )
        if batch_df.count() == 0:
            return

        print(f"\n=== [BATCH {batch_id}] ĐANG PHÂN LÀN DỮ LIỆU ĐA LUỒNG ===")
        batch_df.cache()

        # Đường dẫn kết nối MongoDB
        mongo_uri = "mongodb://admin:secret@mongodb:27017/streaming_analytics.realtime_events?authSource=admin"

        # --- PHÂN NHÁNH 1: XỬ LÝ LOG XEM PHIM (WATCH) ---
        # Lọc trực tiếp bằng cột event_type đã bung sẵn, chọn các cột đúng chuẩn Mô hình sao
        watch_batch = batch_df \
            .filter(col("event_type") == "watch") \
            .select(
                "session_id", "user_id", "movie_id", "device_type", 
                "watch_duration_minutes", "progress_percentage", 
                "action", "quality", "user_rating", "location_country", "date_key"
            )

        if watch_batch.count() > 0:
            # 1. Ghi xuống HDFS (Cold Data)
            print(f" [HDFS] Ghi {watch_batch.count()} dòng vào thư mục fact_watch_history...")
            watch_batch.coalesce(1).write \
                .format("parquet") \
                .mode("append") \
                .partitionBy("date_key") \
                .save(f"{hdfs_namenode}/user/hadoop/warehouse/fact_watch_history")
            # 2. Ghi xuống MongoDB (Hot Data - Phục vụ hiển thị Dashboard tức thời)
            print(f" [MongoDB] Đang đẩy {watch_batch.count()} sự kiện Watch sang NoSQL...")
            watch_batch.write \
                .format("mongodb") \
                .option("connection.uri", "mongodb://admin:secret@mongodb:27017") \
                .option("database", "streaming_analytics") \
                .option("collection", "fact_watch_history") \
                .mode("append") \
                .save()
            watch_batch.show(3, truncate=False)

        # --- PHÂN NHÁNH 2: XỬ LÝ LOG TÌM KIẾM (SEARCH) ---
        # Lọc trực tiếp bằng cột event_type đã bung sẵn, chọn các cột đúng chuẩn Mô hình sao
        search_batch = batch_df \
            .filter(col("event_type") == "search") \
            .select(
                "search_id", "user_id", "search_query", "results_returned", 
                "clicked_result_position", "search_duration_seconds", 
                "had_typo", "device_type", "location_country", "date_key"
            )

        if search_batch.count() > 0:
            # 1. Ghi xuống HDFS (Cold Data)
            print(f" [HDFS] Ghi {search_batch.count()} dòng vào thư mục fact_search_logs...")
            search_batch.coalesce(1).write \
                .format("parquet") \
                .mode("append") \
                .partitionBy("date_key") \
                .save(f"{hdfs_namenode}/user/hadoop/warehouse/fact_search_logs")
            # 2. Ghi xuống MongoDB (Hot Data)
            print(f" [MongoDB] Đang đẩy {search_batch.count()} sự kiện Search sang NoSQL...")
            search_batch.write \
                .format("mongodb") \
                .option("connection.uri", "mongodb://admin:secret@mongodb:27017") \
                .option("database", "streaming_analytics") \
                .option("collection", "fact_search_logs") \
                .mode("append") \
                .save()
            search_batch.show(3, truncate=False)

        batch_df.unpersist()
    
    # 5. Phân tách Json và định hình mốc thời gian (Thực hiện tập trung bên ngoài)
    parsed_stream = raw_socket_stream \
        .select(from_json(col("value"), combined_schema).alias("data")) \
        .select("data.*")
    
    # Làm sạch dữ liệu rác không có user_id và tự sinh khoá thời gian date_key chuẩn hóa YYYYMMDDHH
    refined_stream = parsed_stream \
        .filter(col("user_id").isNotNull()) \
        .withColumn("ts", to_timestamp(col("current_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("date_key", date_format(col("ts"), "yyyyMMddHH"))

    spark.conf.set("spark.sql.adaptive.enabled", "false")
    # 6. KÍCH HOẠT LUỒNG STREAM DUY NHẤT ĐỔ VỀ HDFS
    print(" ĐANG KÍCH HOẠT ĐƯỜNG ỐNG STREAMING ĐỔ VỀ HDFS KHÔNG GIAN LƯU TRỮ...")
    query = refined_stream.writeStream \
        .foreachBatch(process_combined_batch) \
        .option("checkpointLocation", f"{hdfs_namenode}/user/hadoop/checkpoints/combined_stream") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()