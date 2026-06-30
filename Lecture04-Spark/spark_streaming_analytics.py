from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, desc, sum, avg, min, max

# Khởi tạo Spark Session kết nối với Master
spark = SparkSession.builder \
    .appName("Netflix_Clickstream_Analytics") \
    .getOrCreate()

# Tắt bớt log rác để màn hình hiển thị sạch
spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("BẮT ĐẦU CHẠY SPARK")
print("="*60 + "\n")

# 1: INGESTION & CLEANING
print("1: Đang nạp và làm sạch dữ liệu...")
df_raw = spark.read.csv("hdfs://namenode:9000/user/hadoop/input/video_streaming_bigdata.csv", header=True, inferSchema=True)
total_raw = df_raw.count()

# Xóa trùng lặp (Deduplication)
df_dedup = df_raw.dropDuplicates()
total_dedup = df_dedup.count()
removed_duplicates = total_raw - total_dedup

# Lọc khuyết ID hoặc lỗi tiêu đề (Null/Empty)
df_valid_info = df_dedup.filter(
    (col("user_id") != "") & (col("user_id").isNotNull()) & 
    (col("video_title") != "") & (col("video_title").isNotNull())
)
total_valid_info = df_valid_info.count()
removed_missing = total_dedup - total_valid_info

# Lọc thời gian xem vô lý (Watch Time <= 0)
df_valid_time = df_valid_info.filter(col("watch_time_minutes") > 0)
total_valid_time = df_valid_time.count()
removed_invalid_time = total_valid_info - total_valid_time

# Lọc lỗi định dạng ngày tháng (Invalid Timestamp)
df_timestamp = df_valid_time.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
df_cleaned = df_timestamp.dropna(subset=["timestamp"])
total_clean = df_cleaned.count()
removed_invalid_date = total_valid_time - total_clean

print(f"  Tổng số bản ghi thô nạp vào:     {total_raw:>6} dòng")
print(f"  Đã xóa dòng trùng lặp (Dedup):   {removed_duplicates:>6} dòng")
print(f"  Đã xóa dòng khuyết User/Title:   {removed_missing:>6} dòng")
print(f"  Đã xóa dòng có phút xem <= 0:    {removed_invalid_time:>6} dòng")
print(f"  Đã xóa dòng sai định dạng ngày:  {removed_invalid_date:>6} dòng")
print(f"  Tổng số bản ghi SẠCH hợp lệ:    {total_clean:>6} dòng")
print(f"  Tỷ lệ dữ liệu sạch đạt chuẩn:    {(total_clean/total_raw)*100:.2f}%")

# 2: CORE PROCESSING
print("\n2: Tính toán các chỉ số cốt lõi...")
print("\n[TOP 5 MOST ACTIVE USERS]:")
df_cleaned.groupBy("user_id").agg(sum("watch_time_minutes").alias("Total_Minutes")).orderBy(desc("Total_Minutes")).show(5)

print("\n[TOP 5 TRENDING VIDEOS]:")
df_cleaned.groupBy("video_title").count().withColumnRenamed("count", "Views").orderBy(desc("Views")).show(5)

print("\n[PEAK ACTIVITY TIME] - Khung giờ xem phim nhiều nhất:")
df_cleaned.withColumn("hour", hour(col("timestamp"))).groupBy("hour").count().orderBy(desc("count")).show(5)

# 3: AGGREGATION
print("\n3: Gom nhóm theo Thể loại (Genre):")
df_cleaned.groupBy("genre").agg(
    sum("watch_time_minutes").alias("Tong_Phut"),
    avg("watch_time_minutes").alias("Trung_Binh_Phut"),
    min("watch_time_minutes").alias("Ngan_Nhat"),
    max("watch_time_minutes").alias("Dai_Nhat")
).show()

# 4: ADVANCED ANALYSIS (ANOMALY DETECTION)
print("\n4: Phát hiện tài khoản Bot cày view ảo (Xem >= 24 tiếng liên tục):")
df_anomalies = df_cleaned.filter(col("watch_time_minutes") >= 1440)
print(f"Phát hiện {df_anomalies.count()} hành vi bất thường:")
df_anomalies.select("timestamp", "user_id", "video_title", "watch_time_minutes", "device_type").show(5)
# print("Kiểm tra USER_8888:")
# df_anomalies.filter(col("user_id") == "USER_8888").show()
# print("Phát hiện các hành vi bất thường (Top các phiên cày view khủng):")
# df_anomalies.orderBy(desc("watch_time_minutes")).show(10)

print("\n" + "="*60)
print("PIPELINE HOÀN THÀNH")
print("="*60 + "\n")

#input("Mở http://localhost:4040...")

spark.stop()