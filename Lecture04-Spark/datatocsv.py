import csv
import random
import datetime


# 1. Cấu hình Thể loại và Tên Video
genres = {
    "Action": ["John_Wick_4", "Mad_Max_Fury", "Die_Hard", "Extraction_2"],
    "Comedy": ["The_Office_S01", "Friends_S05", "Brooklyn_99", "Superbad"],
    "Sci-Fi": ["Inception", "Interstellar", "The_Matrix", "Black_Mirror_E01"],
    "Documentary": ["Our_Planet", "The_Last_Dance", "Social_Dilemma", "Cosmos"]
}

file_path = "video_streaming_bigdata.csv"
total_rows = 100000  # Quy mô 100k dòng



print(f"🚀 Đang khởi tạo đường ống sinh {total_rows} dòng dữ liệu Video Streaming...")



start_time = datetime.datetime(2026, 6, 1, 0, 0, 0)



with open(file_path, mode="w", newline="", encoding="utf-8") as f:

    writer = csv.writer(f)

    # Header chuẩn hệ thống telemetry của Netflix/YouTube

    writer.writerow(["timestamp", "user_id", "video_id", "genre", "video_title", "watch_time_minutes", "device_type"])

    

    i = 0

    while i < total_rows:

        # Tỷ lệ ngẫu nhiên 5% sinh dữ liệu bẩn (Dirty Data)

        is_dirty = random.random() < 0.05

        if is_dirty:
            dirty_type = random.choice(["missing_user", "negative_time", "corrupt_date", "missing_title"])

            timestamp = (start_time + datetime.timedelta(seconds=random.randint(0, 7 * 24 * 3600))).strftime("%Y-%m-%d %H:%M:%S")
            user_id = f"USER_{random.randint(1000, 2000)}"
            genre = random.choice(list(genres.keys()))
            title = random.choice(genres[genre])
            vid_id = f"VID_{title.upper()}"
            watch_time = random.randint(5, 120)
            device = random.choice(["SmartTV", "Mobile", "Laptop", "Tablet"])

            # Thực hiện cài cắm lỗi
            if dirty_type == "missing_user":
                user_id = ""  # Lỗi thiếu User ID
            elif dirty_type == "negative_time":
                watch_time = random.choice([0, -10, -99])  # Lỗi thời gian vô lý
            elif dirty_type == "corrupt_date":
                timestamp = random.choice(["CORRUPT_TIMESTAMP", "09/06/2026", ""])  # Lỗi sai định dạng ngày
            elif dirty_type == "missing_title":
                title = ""  # Lỗi mất tên video

            writer.writerow([timestamp, user_id, vid_id, genre, title, watch_time, device])
            i += 1

        else:
            # --- SINH DỮ LIỆU SẠCH (95%) ---
            # Giả lập thời gian + Tạo Peak Hour (20h - 23h đêm)
            rand_secs = random.randint(0, 7 * 24 * 3600)
            current_time = start_time + datetime.timedelta(seconds=rand_secs)
            if random.random() < 0.45:  
                current_time = current_time.replace(hour=random.choice([20, 21, 22, 23]))
            timestamp_str = current_time.strftime("%Y-%m-%d %H:%M:%S")

            # Giả lập Người xem + Tạo Most Active Users
            if random.random() < 0.10:
                user_id = f"USER_{random.choice([8888, 9999])}"
            else:
                user_id = f"USER_{random.randint(1000, 2000)}"

            # Giả lập Phim & Thể loại
            genre = random.choice(list(genres.keys()))
            title = random.choice(genres[genre])
            vid_id = f"VID_{title.upper()}"
            watch_time = int(random.expovariate(1/35))
            watch_time = max(1, min(watch_time, 180))

            # Cài cắm Bất thường (Anomaly Detection cho Task 4): Bot cày view ảo (Xem 24 tiếng liên tục)
            if random.random() < 0.003:
                watch_time = random.randint(1440, 2000)
            device = random.choice(["SmartTV", "Mobile", "Laptop", "Tablet"])
            writer.writerow([timestamp_str, user_id, vid_id, genre, title, watch_time, device])
            i += 1

            # Giả lập lỗi trùng lặp dữ liệu (Duplicate Rows)
            if random.random() < 0.01 and i < total_rows:
                writer.writerow([timestamp_str, user_id, vid_id, genre, title, watch_time, device])
                i += 1

print(f"🎯 Hoàn thành! Đã tạo ra file {total_rows} dòng tại: {file_path}")