import csv
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ========================================================
# CẤU HÌNH KẾT NỐI KAFKA KRAFT
# ========================================================
# Khi chạy script từ máy ngoài Docker (localhost), ta kết nối qua cổng EXTERNAL (9094)
KAFKA_BROKER = 'localhost:9094'  
KAFKA_TOPIC = 'video-clickstream'
DELAY_SECONDS = 0.05  # Tốc độ bắn luồng (20 sự kiện / giây)

print("============================================================")
print(" ĐANG KHỞI TẠO HỆ THỐNG GIẢ LẬP KAFKA STREAMING PRODUCER...")
print("============================================================")

# Cấu hình Producer tự động mã hóa dữ liệu dict/json thành chuỗi bytes UTF-8 trước khi bắn qua Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Nạp dữ liệu thô vào bộ nhớ
try:
    with open('watch_history.csv', mode='r', encoding='utf-8') as f:
        watch_pool = list(csv.DictReader(f))
    with open('search_logs.csv', mode='r', encoding='utf-8') as f:
        search_pool = list(csv.DictReader(f))
except FileNotFoundError as e:
    print(f" Lỗi: Không tìm thấy file dữ liệu CSV mẫu. Vui lòng kiểm tra lại đường dẫn! \n{e}")
    exit(1)
# data cleaning
def clean_watch_event(raw):
    return {
        "session_id": raw.get("session_id"),
        "user_id": raw.get("user_id") if raw.get("user_id") else None,
        "movie_id": raw.get("movie_id"),
        "device_type": raw.get("device_type"),
        "watch_duration_minutes": float(raw.get("watch_duration_minutes")) if raw.get("watch_duration_minutes") else 0.0,
        # "progress_percentage": float(raw.get("progress_percentage")) if raw.get("progress_percentage") else 0.0,
        "progress_percentage": 0.0,
        "action": raw.get("action"),
        "quality": raw.get("quality"),
        "user_rating": int(raw.get("user_rating")) if raw.get("user_rating") and raw.get("user_rating").isdigit() else None,
        "location_country": raw.get("location_country", "Unknown")
    }

def clean_search_event(raw):
    return {
        "search_id": raw.get("search_id"),
        "user_id": raw.get("user_id") if raw.get("user_id") else None,
        "search_query": raw.get("search_query"),
        "results_returned": int(raw.get("results_returned")) if raw.get("results_returned") else 0,
        "clicked_result_position": int(raw.get("clicked_result_position")) if raw.get("clicked_result_position") else None,
        "search_duration_seconds": float(raw.get("search_duration_seconds")) if raw.get("search_duration_seconds") else 0.0,
        "had_typo": raw.get("had_typo", "False"),
        "device_type": raw.get("device_type"),
        "location_country": raw.get("location_country", "Unknown")
    }
# lấy mốc thời gian hiện tại làm mốc tính toán
base = datetime.now()
def start_simulator():
    print(f" KAFKA SYSTEM ACTIVE: Đang đẩy dữ liệu liên tục vào Topic [{KAFKA_TOPIC}]...")
    
    count_watch = 0
    count_search = 0
    count_total = 0
    start_time = time.time()
    
    try:
        while True:
            # Tỷ lệ 70% log xem phim, 30% log tìm kiếm phim
            if random.random() < 0.7:
                raw_event = random.choice(watch_pool)
                data_event = clean_watch_event(raw_event)
                data_event['event_type'] = 'watch'
                count_watch += 1
            else:
                raw_event = random.choice(search_pool)
                data_event = clean_search_event(raw_event)
                data_event['event_type'] = 'search'
                count_search += 1
            
            # Giả lập trong vòng 7 ngày qua (60 phút * 24 giờ * 7 ngày = 4320 phút)
            random_minutes = random.randint(0, 60 * 24 * 7)
            event_time = base - timedelta(minutes=random_minutes)

            data_event["current_timestamp"] = event_time.strftime("%Y-%m-%d %H:%M:%S")
            count_total += 1
            
            # GỬI SỰ KIỆN QUA KAFKA
            producer.send(KAFKA_TOPIC, data_event)
            
            # In giám sát màn hình terminal (Real-time tracking console)
            elapsed_time = time.time() - start_time
            eps = count_total / elapsed_time if elapsed_time > 0 else 0
            print(f"\r[KAFKA PRODUCER] Đã phát: {count_total:>6} Events | 🎬 Watch: {count_watch:>5} | 🔍 Search: {count_search:>5} | Tốc độ: {eps:.1f} ev/s", end="")
            
            time.sleep(DELAY_SECONDS)
            
    except KeyboardInterrupt:
        print("\n Đã dừng Simulator")
    finally:
        producer.close()

if __name__ == "__main__":
    start_simulator()