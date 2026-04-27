"""Streamlit前端主入口，配置页面结构和导航"""
import sys
import os

# 将项目根目录添加到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from app.database import init_database
from app.services.data_collector import generate_sample_data
from app.ml.win_loss_model import train_and_save_all_models
from app.ml.goals_model import train_and_save_goals_models

# 页面配置
st.set_page_config(
    page_title="ProphitBet 竞彩预测",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 自定义CSS样式
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1a5276;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #5d6d7e;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .prob-home { color: #27ae60; font-weight: bold; }
    .prob-draw { color: #f39c12; font-weight: bold; }
    .prob-away { color: #e74c3c; font-weight: bold; }
    .score-highlight {
        font-size: 2rem;
        font-weight: 800;
        color: #2c3e50;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


def ensure_data_initialized():
    """确保数据库已初始化并有数据"""
    if "data_initialized" not in st.session_state:
        init_database()
        st.session_state.data_initialized = True

    if "sample_data_loaded" not in st.session_state:
        from app.database import get_connection
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as cnt FROM teams")
            row = cursor.fetchone()
            if row["cnt"] == 0:
                with st.spinner("正在生成示例数据，请稍候..."):
                    generate_sample_data(seasons=3)
                st.session_state.sample_data_loaded = True
            else:
                st.session_state.sample_data_loaded = True
        finally:
            conn.close()


def ensure_models_trained():
    """确保模型已训练"""
    if "models_trained" not in st.session_state:
        from app.config import MODEL_DIR
        model_exists = (
            os.path.exists(os.path.join(MODEL_DIR, "win_loss_xgboost.joblib")) and
            os.path.exists(os.path.join(MODEL_DIR, "goals_xgboost.joblib"))
        )
        if not model_exists:
            with st.spinner("正在训练预测模型，首次运行可能需要几分钟..."):
                try:
                    train_and_save_all_models()
                    train_and_save_goals_models()
                except Exception as e:
                    st.warning(f"模型训练出现问题: {e}，将使用默认预测")
        st.session_state.models_trained = True


# 主页面
def main():
    """主页面入口"""
    ensure_data_initialized()
    ensure_models_trained()

    st.markdown('<div class="main-header">⚽ ProphitBet 竞彩预测系统</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">基于因子化监督学习的足球比赛概率预测平台</div>', unsafe_allow_html=True)

    st.markdown("---")

    # 功能导航
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 🔮 比赛预测")
        st.write("选择比赛，获取胜负概率、比分预测、大小球分析")
        if st.button("进入预测 →", key="btn_predict", use_container_width=True):
            st.switch_page("pages/1_🔮_预测.py")

    with col2:
        st.markdown("### 📊 数据分析")
        st.write("查看因子分析、球队数据对比、联赛积分榜")
        if st.button("进入分析 →", key="btn_analysis", use_container_width=True):
            st.switch_page("pages/2_📊_数据分析.py")

    with col3:
        st.markdown("### ✅ 历史验证")
        st.write("查看模型历史表现、预测准确率统计")
        if st.button("进入验证 →", key="btn_validate", use_container_width=True):
            st.switch_page("pages/3_✅_历史验证.py")

    st.markdown("---")

    # 系统概览
    st.markdown("### 📋 系统概览")

    from app.database import get_connection
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # 统计数据
        cursor.execute("SELECT COUNT(*) as cnt FROM teams")
        team_count = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) as cnt FROM matches")
        match_count = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(*) as cnt FROM matches WHERE result IS NOT NULL")
        completed_count = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(DISTINCT league_id) as cnt FROM leagues")
        league_count = cursor.fetchone()["cnt"]

        cursor.execute("SELECT COUNT(DISTINCT season) as cnt FROM matches")
        season_count = cursor.fetchone()["cnt"]

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("球队数量", f"{team_count}")
        col2.metric("比赛记录", f"{match_count}")
        col3.metric("已完成比赛", f"{completed_count}")
        col4.metric("覆盖联赛", f"{league_count}")
        col5.metric("覆盖赛季", f"{season_count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
