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
        # Không dùng st.error ở đây để tránh lỗi giao diện lan sang các tab khác
        return pd.DataFrame()

# ============================================================
# 3. TIÊU ĐỀ CHÍNH & KHỐI KPI TỔNG QUAN (TOP BAR)
# ============================================================
st.title("🎬 Video Streaming Platform - User Behavior Analytics")
st.markdown("*Hệ thống phân tích hành vi người dùng bằng Spark MLlib, MongoDB và Streamlit*")
st.caption("Kiến trúc hệ thống: Kafka ➡️ Spark Structured Streaming ➡️ HDFS ➡️ Spark MLlib ➡️ MongoDB ➡️ Streamlit")
st.write("---")

# Tải dữ liệu phân cụm ML làm gốc tính toán KPI
df_ml = load_collection_data("ml_user_segments")

if df_ml.empty:
    st.warning("⚠️ Serving Layer hiện tại chưa có dữ liệu phân cụm `ml_user_segments`. Vui lòng chạy file ML trước.")
    st.stop()

# Hiển thị 3 chỉ số KPI cốt lõi ở trên cùng Dashboard
total_users = len(df_ml)
avg_duration = df_ml["total_duration_minutes"].mean()
avg_progress = df_ml["avg_progress_percentage"].mean()

kpi1, kpi2, kpi3 = st.columns(3)
with kpi1:
    st.metric("👥 Tổng quy mô người dùng", f"{total_users:,} User")
with kpi2:
    st.metric("⏱️ Thời lượng xem trung bình", f"{avg_duration:.1f} phút")
with kpi3:
    st.metric("📈 Tiến trình xem trung bình", f"{avg_progress:.1f}%")

st.write("---")

# ============================================================
# 4. CHIA CÓC TRÚC 3 TAB THEO ĐÚNG BẢN KẾ HOẠCH
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
    st.markdown("*(Dữ liệu được Spark Streaming liên tục xử lý từ Kafka và đẩy về MongoDB)*")
    
    col_t1_left, col_t1_right = st.columns(2)
    
    with col_t1_left:
        st.subheader("🔥 Top Thể Loại Phim Đang Hot")
        # Đọc dữ liệu từ collection report_genre_popularity của bạn
        df_genre = load_collection_data("report_genre_popularity")
        if not df_genre.empty:
            # Tự động lấy tên cột đầu tiên và cột số lượng để vẽ
            cols = df_genre.columns.tolist()
            fig_genre = px.bar(
                df_genre, x=cols[0], y=cols[1],
                labels={cols[0]: "Thể loại", cols[1]: "Lượt xem"},
                color=cols[0], color_discrete_sequence=px.colors.qualitative.Pastel
            )
            st.plotly_chart(fig_genre, use_container_width=True)
        else:
            st.info("💡 Chưa có dữ liệu thời gian thực cho thể loại phim.")

    with col_t1_right:
        st.subheader("📱 Cơ Cấu Thiết Bị Truy Cập")
        # Đọc dữ liệu từ collection report_top_devices của bạn
        df_device = load_collection_data("report_top_devices")
        if not df_device.empty:
            cols = df_device.columns.tolist()
            fig_device = px.pie(
                df_device, names=cols[0], values=cols[1],
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
    
    # Chia làm 2 cột cho Pie Chart và Scatter Plot giống như thiết kế cũ của bạn
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