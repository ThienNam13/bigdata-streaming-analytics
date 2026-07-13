# ============================================================
# FILE: dashboard.py
# TẦNG TRỰC QUAN HÓA DỮ LIỆU (Data Visualization Layer)
#
# Hệ thống hiển thị và giám sát phân khúc người dùng Video Streaming
# ============================================================

import streamlit as st
import pandas as pd
import pymongo
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# ============================================================
# 1. CẤU HÌNH TRANG GIAO DIỆN (PAGE CONFIG)
# ============================================================
st.set_page_config(
    page_title="Streaming User Behavior Analytics",
    page_icon="🎬",
    layout="wide"
)
# Tự động refresh mỗi 15 giây
st_autorefresh(interval=15 * 1000, key="data_refresh")

# Cấu hình kết nối cơ sở dữ liệu MongoDB Serving Layer
MONGO_URI = "mongodb://admin:secret@localhost:27017"
DATABASE = "streaming_analytics"

# ============================================================
# 2. HÀM ĐỌC DỮ LIỆU ĐA NĂNG TỪ CÁC COLLECTION (HAVE CACHING)
# ============================================================
@st.cache_data(ttl=14)  # Bộ nhớ đệm 14 giây để cập nhật nhanh dữ liệu Streaming
def load_collection_data(collection_name):
    """
    Hàm tổng quát kết nối vào MongoDB và lấy dữ liệu của một Collection cụ thể chuyển thành DataFrame
    """
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        db = client[DATABASE]
        data = list(db[collection_name].find({}, {"_id": 0}))
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
    except Exception as e:
        return pd.DataFrame()

# ============================================================
# 3. TIÊU ĐỀ CHÍNH & KHỐI KPI TỔNG QUAN (TOP BAR)
# ============================================================
st.title("🎬 Video Streaming Platform - User Behavior Analytics")
st.markdown("*Hệ thống phân tích hành vi người dùng bằng Spark MLlib, MongoDB và Streamlit*")
st.caption("Kiến trúc hệ thống: Kafka ➡️ Spark Structured Streaming ➡️ HDFS ➡️ Spark MLlib ➡️ MongoDB ➡️ Streamlit")
st.write("---")

# Nếu chưa chạy ML, gán các chỉ số KPI là "Đang tính toán..."
df_ml = load_collection_data("ml_user_segments")

if not df_ml.empty:
    total_users = len(df_ml)
    avg_duration = df_ml["total_duration_minutes"].mean()
    avg_progress = df_ml["avg_progress_percentage"].mean()
    
    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1: st.metric("👥 Tổng quy mô người dùng", f"{total_users:,} User")
    with kpi2: st.metric("⏱️ Thời lượng xem trung bình", f"{avg_duration:.1f} phút")
    with kpi3: st.metric("📈 Tiến trình xem trung bình", f"{avg_progress:.1f}%")
else:
    # Hiển thị trạng thái chờ nếu chưa có dữ liệu Batch Layer
    kpi1, kpi2, kpi3 = st.columns(3)
    with kpi1: st.metric("👥 Tổng quy mô người dùng", "Chờ Batch...")
    with kpi2: st.metric("⏱️ Thời lượng xem trung bình", "Chờ Batch...")
    with kpi3: st.metric("📈 Tiến trình xem trung bình", "Chờ Batch...")

st.write("---")

# ============================================================
# 4. CHIA CẤU TRÚC 3 TAB
# ============================================================
tab1, tab2, tab3 = st.tabs([
    "📺 Tab 1: Real-time Monitor", 
    "📊 Tab 2: Business Intelligence Report", 
    "🧠 Tab 3: AI/ML Strategy"
])

# ============================================================
# TAB 1: REAL-TIME MONITOR (Giám sát thời gian thực từ Spark Streaming)
# ============================================================
with tab1:
    st.header("⚡ Hệ Thống Giám Sát Luồng Xem Phim Thời Gian Thực")
    st.markdown("*(Ứng dụng mô hình Lambda: Kết hợp dữ liệu lịch sử và dữ liệu thời gian thực từ Kafka)*")
    
    col_t1_left, col_t1_right = st.columns(2)
    
    with col_t1_left:
        st.subheader("🔥 Top Thể Loại Phim Đang Hot")
        # Đọc dữ liệu từ collection report_genre_popularity (Phân tích thể loại phim được xem nhiều nhất)
        df_genre = load_collection_data("report_genre_popularity")
        df_genre_stream = load_collection_data("report_genre_popularity_stream")
        
        # Gộp dữ liệu bằng Pandas để tránh xung đột Overwrite của Spark Streaming
        df_genre_list = [df_genre, df_genre_stream]
        df_genre_active = [df for df in df_genre_list if not df.empty]
        
        if df_genre_active:
            # Nối và gộp nhóm tính tổng
            df_genre_combined = pd.concat(df_genre_active, ignore_index=True)
            col_name = df_genre_combined.columns[0]  # Thường là 'genre' hoặc 'genre_primary'
            col_value = df_genre_combined.columns[1] # Thường là 'count' hoặc 'total_views'
            
            df_genre_final = df_genre_combined.groupby(col_name)[col_value].sum().reset_index()
            df_genre_final = df_genre_final.sort_values(by=col_value, ascending=False)
            
            fig_genre = px.bar(
                df_genre_final, x=col_name, y=col_value,
                labels={col_name: "Thể loại", col_value: "Tổng lượt xem (Batch + Stream)"},
                color=col_name, color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig_genre, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu thể loại phim từ hệ thống.")

    with col_t1_right:
        st.subheader("📱 Cơ Cấu Thiết Bị Truy Cập")
        # Đọc dữ liệu từ collection report_top_devices (Phân tích thiết bị phổ biến mà người dùng xem phim)
        df_device = load_collection_data("report_top_devices")
        df_device_stream = load_collection_data("report_top_devices_stream")
        
        df_device_list = [df_device, df_device_stream]
        df_device_active = [df for df in df_device_list if not df.empty]
        
        if df_device_active:
            # Nối và gộp nhóm tính tổng
            df_device_combined = pd.concat(df_device_active, ignore_index=True)
            col_name = df_device_combined.columns[0]  # Thường là 'device_type'
            col_value = df_device_combined.columns[1] # Thường là 'count' hoặc 'total_views'
            
            df_device_final = df_device_combined.groupby(col_name)[col_value].sum().reset_index()
            
            fig_device = px.pie(
                df_device_final, names=col_name, values=col_value,
                hole=0.4, color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_device, use_container_width=True)
        else:
            st.info("💡 Chưa có dữ liệu thời gian thực cho thiết bị.")

# ============================================================
# TAB 2: BUSINESS INTELLIGENCE REPORT (Phân tích Batch & Vận hành từ Spark SQL)
# ============================================================
with tab2:
    st.header("📊 Báo Cáo Thống Kê Vận Hành & Rủi Ro Hệ Thống")
    st.markdown("*(Dữ liệu phân tích chuyên sâu được tổng hợp định kỳ qua Spark SQL)*")
    
    # Khu vực 1: Phân tích rủi ro nghẽn mạng (Buffering Churn Risk)
    st.subheader("⚠️ Phân Tích Khách Hàng Có Nguy Cơ Rời Bỏ Do Nghẽn Mạng")
    df_churn = load_collection_data("report_churn_risk")
    if not df_churn.empty:
        st.dataframe(df_churn, use_container_width=True)
    else:
        st.info("💡 Chưa ghi nhận dữ liệu phân tích churn risk.")
        
    st.write("---")
    
    col_t2_left, col_t2_right = st.columns(2)
    
    with col_t2_left:
        st.subheader("⏰ Khung Giờ Xem Phim Cao Điểm (Peak Hours)")
        df_peak = load_collection_data("report_peak_hours")
        if not df_peak.empty:
            cols = df_peak.columns.tolist()
            df_peak = df_peak.sort_values(by=cols[0])
            fig_peak = px.line(df_peak, x=cols[0], y=cols[1], markers=True, title="Lượt truy cập theo giờ")
            fig_peak.update_layout(xaxis=dict(tickmode='linear', dtick=1))
            st.plotly_chart(fig_peak, use_container_width=True)
        else:
            st.info("💡 Chưa có dữ liệu thống kê khung giờ cao điểm.")
            
    with col_t2_right:
        st.subheader("🔍 Từ Khóa Tìm Kiếm Bị Lỗi / Không Ra Kết Quả")
        df_failed_search = load_collection_data("report_failed_searches")
        if not df_failed_search.empty:
            st.dataframe(df_failed_search, use_container_width=True)
        else:
            st.info("💡 Hệ thống vận hành tốt, chưa phát hiện log tìm kiếm lỗi.")

# ============================================================
# TAB 3: AI/ML STRATEGY (Kết quả phân cụm từ thuật toán KMeans)
# ============================================================
with tab3:
    st.header("🧠 Chiến Lược Phân Khúc Người Dùng Bằng AI (Spark MLlib)")
    st.markdown("*(Mô hình K-Means tự động nhận diện thói quen hành vi để tối ưu hóa chiến dịch Marketing)*")
    
    if df_ml.empty:
        st.warning("⚠️ Tab này yêu cầu dữ liệu từ Batch Layer.")
    else:
    # Chia làm 2 cột cho Pie Chart và Scatter Plot
        left_col, right_col = st.columns([1, 1])
    
        with left_col:
            st.subheader("📊 Tỷ Trọng Phân Bổ Các Nhóm")
            segment_counts = df_ml["segment_name"].value_counts().reset_index()
            segment_counts.columns = ["Phân khúc", "Số lượng"]
        
            fig_pie = px.pie(
                segment_counts, values="Số lượng", names="Phân khúc",
                hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with right_col:
            st.subheader("🌙 Biểu Đồ Phân Cụm Hành Vi Thực Tế")
            fig_scatter = px.scatter(
                df_ml, x="total_duration_minutes", y="night_view_ratio",
                size="total_views", color="segment_name", hover_data=["user_id"],
                opacity=0.75,
                labels={
                    "total_duration_minutes": "Tổng số phút xem",
                    "night_view_ratio": "Tỷ lệ xem ban đêm",
                    "segment_name": "Phân khúc"
                }
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        st.write("---")
    
    # Hiển thị bảng số liệu Profile chi tiết của các tâm cụm
    st.subheader("🔍 Chi Tiết Đặc Trưng Chỉ Số Của Từng Cụm")
    cluster_profile = (
        df_ml.groupby("segment_name")
        .agg({
            "total_views": "mean",
            "total_duration_minutes": "mean",
            "avg_progress_percentage": "mean",
            "night_view_ratio": "mean"
        }).reset_index()
    )
    cluster_profile.columns = ["Phân khúc", "Lượt xem TB", "Số phút xem TB", "Tiến trình xem TB (%)", "Tỷ lệ xem đêm TB"]
    
    st.dataframe(
        cluster_profile.style.format({
            "Lượt xem TB": "{:.1f} lượt",
            "Số phút xem TB": "{:.1f} phút",
            "Tiến trình xem TB (%)": "{:.1f}%",
            "Tỷ lệ xem đêm TB": "{:.2%}"
        }),
        use_container_width=True
    )
    
    st.write("---")
    
    # Tính năng Tra cứu danh sách Người dùng (User Lookup)
    st.subheader("📋 Công Cụ Tra Cứu Danh Sách Thành Viên")
    segments_options = ["Tất cả"] + sorted(df_ml["segment_name"].unique().tolist())
    selected_segment = st.selectbox("Chọn phân khúc muốn lọc hành vi:", segments_options)
    
    if selected_segment == "Tất cả":
        filtered_df = df_ml
    else:
        filtered_df = df_ml[df_ml["segment_name"] == selected_segment]
        
    st.dataframe(
        filtered_df[["user_id", "total_views", "total_duration_minutes", "avg_progress_percentage", "night_view_ratio", "segment_name"]],
        use_container_width=True
    )