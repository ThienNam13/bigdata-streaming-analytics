from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, desc, sum, count, avg, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# KHỞI TẠO SPARK ENGINE & ĐƯỜNG DẪN CẤU HÌNH (LOCAL WINDOWS ENVIRONMENT)
# Khởi tạo Spark tích hợp Delta Lake Core cho môi trường chạy Local
spark = SparkSession.builder \
    .appName("Olist_ECommerce_Lakehouse_Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Cấu hình các đường dẫn làm việc
SOURCE_DIR = "D:/Bigdata/Lecture07-Lakehouse"
TARGET_DIR = "D:/Bigdata/Lecture07-Lakehouse/lakehouse_storage"

paths = {
    "csv_orders": f"{SOURCE_DIR}/olist_orders_dataset.csv",
    "csv_items": f"{SOURCE_DIR}/olist_order_items_dataset.csv",
    "csv_products": f"{SOURCE_DIR}/olist_products_dataset.csv",
    "csv_reviews": f"{SOURCE_DIR}/olist_order_reviews_dataset.csv",
    
    # Định vị các phân vùng lưu trữ theo cấu trúc Medallion (Delta format)
    "bronze_orders": f"{TARGET_DIR}/bronze/orders",
    "bronze_items": f"{TARGET_DIR}/bronze/items",
    "bronze_products": f"{TARGET_DIR}/bronze/products",
    "bronze_reviews": f"{TARGET_DIR}/bronze/reviews",
    
    "silver_orders": f"{TARGET_DIR}/silver/orders",
    "silver_items": f"{TARGET_DIR}/silver/items",
    "silver_products": f"{TARGET_DIR}/silver/products",
    "silver_reviews": f"{TARGET_DIR}/silver/reviews",
    
    "gold_sales_kpi": f"{TARGET_DIR}/gold/sales_daily_kpi",
    "gold_category_performance": f"{TARGET_DIR}/gold/category_performance",
    "gold_product_satisfaction": f"{TARGET_DIR}/gold/product_satisfaction"
}

print("=== [SẴN SÀNG] Đã khởi tạo cấu hình hệ thống Lakehouse ===")



# THU THẬP VÀ NẠP DỮ LIỆU GỐC (BRONZE LAYER - RAW LANDING ZONE)
print("\n--- TIẾN TRÌNH 1: ĐANG NẠP DỮ LIỆU THÔ VÀO TẦNG BRONZE ---")

# Đọc trực tiếp các file CSV thô được cấu trúc chuỗi (Mọi cột đều là String và giữ nguyên bản)
df_raw_orders = spark.read.option("header", "true").csv(paths["csv_orders"])
df_raw_items = spark.read.option("header", "true").csv(paths["csv_items"])
df_raw_products = spark.read.option("header", "true").csv(paths["csv_products"])
df_raw_reviews = spark.read.option("header", "true").csv(paths["csv_reviews"])

# Ghi lưu trữ nguyên trạng vào tầng Bronze dưới dạng Delta Table (Lưu trữ lịch sử dữ liệu gốc)
df_raw_orders.write.format("delta").mode("overwrite").save(paths["bronze_orders"])
df_raw_items.write.format("delta").mode("overwrite").save(paths["bronze_items"])
df_raw_products.write.format("delta").mode("overwrite").save(paths["bronze_products"])
df_raw_reviews.write.format("delta").mode("overwrite").save(paths["bronze_reviews"])

print("-> [SUCCESS] Hoàn thành lưu trữ dữ liệu thô tại tầng BRONZE.")



# LÀM SẠCH, XÓA DỮ LIỆU LỖI & ÉP SCHEMA (SILVER LAYER - CLEANED ZONE)
print("\n--- TIẾN TRÌNH 2: ĐANG XỬ LÝ LÀM SẠCH VÀ ĐỒNG BỘ TẦNG SILVER ---")

# Đọc ngược dữ liệu từ tầng Bronze lên để xử lý tính toán chuyên sâu
bronze_orders = spark.read.format("delta").load(paths["bronze_orders"])
bronze_items = spark.read.format("delta").load(paths["bronze_items"])
bronze_products = spark.read.format("delta").load(paths["bronze_products"])
bronze_reviews = spark.read.format("delta").load(paths["bronze_reviews"])

# 1. Làm sạch bảng Orders: xÓA lặp khóa chính và chuẩn hóa định dạng thời gian mua hàng
silver_orders = bronze_orders \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("silver_processed_at", current_timestamp())

# 2. Làm sạch bảng Items: Ép kiểu số thực cho Price/Freight và lọc bỏ các bản ghi lỗi biên độ âm
silver_items = bronze_items \
    .withColumn("price", col("price").cast(DoubleType())) \
    .withColumn("freight_value", col("freight_value").cast(DoubleType())) \
    .filter((col("price") >= 0) & (col("freight_value") >= 0)) \
    .withColumn("silver_processed_at", current_timestamp())

# 3. Làm sạch bảng Products: Xử lý các giá trị danh mục trống (Null/Blank) thành 'unknown'
silver_products = bronze_products \
    .withColumn("product_category_name", coalesce(col("product_category_name"), lit("unknown"))) \
    .withColumn("silver_processed_at", current_timestamp())

# 4. Làm sạch bảng Reviews: Ép điểm đánh giá về kiểu Số nguyên tiêu chuẩn
silver_reviews = bronze_reviews \
    .withColumn("review_score", col("review_score").cast(IntegerType())) \
    .filter(col("review_score").isNotNull()) \
    .withColumn("silver_processed_at", current_timestamp())

# Ghi dữ liệu sạch vào tầng Silver
silver_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "false").save(paths["silver_orders"])
silver_items.write.format("delta").mode("overwrite").option("overwriteSchema", "false").save(paths["silver_items"])
silver_products.write.format("delta").mode("overwrite").option("overwriteSchema", "false").save(paths["silver_products"])
silver_reviews.write.format("delta").mode("overwrite").option("overwriteSchema", "false").save(paths["silver_reviews"])

print("-> [SUCCESS] Đã đồng bộ dữ liệu sạch, xử lý triệt để Null và Duplicates lên tầng SILVER.")



# TÍNH TOÁN CHỈ SỐ DOANH NGHIỆP (GOLD LAYER - KHO PHÂN TÍCH TIN CẬY)
print("\n--- TIẾN TRÌNH 3: ĐANG TỔNG HỢP CÁC CHỈ SỐ KINH DOANH LÊN TẦNG GOLD ---")

# Đọc dữ liệu đáng tin cậy hoàn toàn từ lớp Silver
s_orders = spark.read.format("delta").load(paths["silver_orders"])
s_items = spark.read.format("delta").load(paths["silver_items"])
s_products = spark.read.format("delta").load(paths["silver_products"])
s_reviews = spark.read.format("delta").load(paths["silver_reviews"])


# KPI 1: Báo cáo Doanh thu tổng hợp theo ngày (Sales KPI)
gold_sales_daily = s_orders \
    .join(s_items, "order_id", "inner") \
    .groupBy(col("order_purchase_timestamp").cast("date").alias("sales_date")) \
    .agg(
        sum("price").alias("total_item_revenue"),
        sum("freight_value").alias("total_freight_cost"),
        count("order_id").alias("total_order_items_sold")
    ) \
    .orderBy("sales_date")

# KPI 2: Hiệu suất kinh doanh theo Danh mục sản phẩm (Product Performance)
gold_category_perf = s_items \
    .join(s_products, "product_id", "inner") \
    .groupBy("product_category_name") \
    .agg(
        sum("price").alias("total_category_revenue"),
        count("product_id").alias("total_units_sold")
    ) \
    .orderBy(desc("total_category_revenue"))

# KPI 3: Phân tích mức độ hài lòng/Phản hồi khách hàng (Customer Satisfaction KPI)
gold_product_satisfaction = s_reviews \
    .join(s_items, "order_id", "inner") \
    .join(s_products, "product_id", "inner") \
    .groupBy("product_category_name") \
    .agg(
        avg("review_score").alias("avg_customer_rating"),
        count("review_id").alias("total_reviews_received")
    ) \
    .orderBy(desc("avg_customer_rating"))

# Xuất bản dữ liệu phân tích cuối cùng ra tầng Gold để Power BI / Tableau truy cập trực tiếp
gold_sales_daily.write.format("delta").mode("overwrite").save(paths["gold_sales_kpi"])
gold_category_perf.write.format("delta").mode("overwrite").save(paths["gold_category_performance"])
gold_product_satisfaction.write.format("delta").mode("overwrite").save(paths["gold_product_satisfaction"])

print("-> [SUCCESS] Đã kết xuất các bảng chỉ số nghiệp vỤ lên tầng GOLD.")



# ĐÁNH GIÁ CHẤT LƯỢNG DỮ LIỆU ĐẦU RA (DATA QUALITY VERIFICATION)
print("\n" + "="*50)
print("=== KIỂM TRA MẪU DỮ LIỆU ĐẦU RA THỰC TẾ TẠI TẦNG GOLD ===")
print("="*50)

print("\n[BẢNG 1: DOANH THU THEO NGÀY (DAILY SALES KPI)]")
spark.read.format("delta").load(paths["gold_sales_kpi"]).show(5)

print("\n[BẢNG 2: TOP DOANH THU THEO DANH MỤC (CATEGORY PERFORMANCE)]")
spark.read.format("delta").load(paths["gold_category_performance"]).show(5)

print("\n[BẢNG 3: ĐÁNH GIÁ SỰ HÀI LÒNG THEO DANH MỤC (CUSTOMER RATINGS)]")
spark.read.format("delta").load(paths["gold_product_satisfaction"]).show(5)

print("\n=== [HOÀN THÀNH] TOÀN BỘ PIPELINE LAKEHOUSE ĐÃ CHẠY XONG ===")