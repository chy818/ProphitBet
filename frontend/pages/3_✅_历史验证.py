"""历史验证页面 - 模型表现和预测准确率统计"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from app.database import get_connection, get_all_leagues
from app.services.prediction_service import get_model_performance, get_historical_accuracy
from app.ml.win_loss_model import train_and_save_all_models
from app.ml.goals_model import train_and_save_goals_models

st.set_page_config(page_title="历史验证", page_icon="✅", layout="wide")

st.title("✅ 历史验证")
st.markdown("查看模型历史表现和预测准确率统计")


# 标签页
tab1, tab2, tab3 = st.tabs(["🎯 模型性能", "📊 准确率统计", "🔄 重新训练"])


with tab1:
    """模型性能"""
    st.subheader("模型性能概览")

    performance = get_model_performance()

    for model_name, info in performance.items():
        with st.expander(f"📦 {model_name}", expanded=True):
            if info["exists"]:
                st.success("✅ 模型已训练")
                st.text(f"路径: {info['path']}")
            else:
                st.warning("⚠️ 模型未训练，请前往「重新训练」标签页训练模型")

    # 模型评估指标说明
    st.markdown("---")
    st.markdown("### 📖 评估指标说明")
    st.markdown("""
    | 指标 | 说明 |
    |------|------|
    | 准确率 (Accuracy) | 预测正确的比例 |
    | F1-Score (Macro) | 各类别F1值的平均，适合不平衡数据 |
    | 对数损失 (Log Loss) | 预测概率与真实标签的偏差，越小越好 |
    | RMSE | 均方根误差，用于进球数预测评估 |
    | MAE | 平均绝对误差，用于进球数预测评估 |
    """)


with tab2:
    """准确率统计"""
    st.subheader("历史预测准确率")

    conn = get_connection()
    try:
        # 筛选条件
        col1, col2 = st.columns(2)
        with col1:
            leagues = get_all_leagues(conn)
            league_options = {"全部": None}
            for lg in leagues:
                league_options[lg["league_name"]] = lg["league_id"]
            selected_league = st.selectbox("联赛筛选", list(league_options.keys()), key="acc_league")

        with col2:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT season FROM matches ORDER BY season DESC")
            seasons = ["全部"] + [row["season"] for row in cursor.fetchall()]
            selected_season = st.selectbox("赛季筛选", seasons, key="acc_season")

        if st.button("📈 查看准确率", key="acc_btn"):
            league_id = league_options[selected_league]
            season = None if selected_season == "全部" else selected_season

            accuracy_data = get_historical_accuracy(league_id=league_id, season=season)

            if accuracy_data["total"] > 0:
                # 总体指标
                col1, col2, col3 = st.columns(3)
                col1.metric("总预测场次", accuracy_data["total"])
                col2.metric("正确预测", accuracy_data["correct"])
                col3.metric("准确率", f"{accuracy_data['accuracy']:.1%}")

                # 详细预测记录
                if accuracy_data["details"]:
                    details_df = pd.DataFrame(accuracy_data["details"])
                    st.dataframe(details_df, use_container_width=True)

                    # 结果分布
                    result_counts = details_df["correct"].value_counts()
                    fig = go.Figure(data=[go.Pie(
                        labels=["预测正确", "预测错误"],
                        values=[result_counts.get(True, 0), result_counts.get(False, 0)],
                        marker=dict(colors=["#27ae60", "#e74c3c"]),
                        hole=0.4,
                    )])
                    fig.update_layout(title="预测结果分布", height=400)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无历史预测数据。请先进行比赛预测。")
    finally:
        conn.close()


with tab3:
    """重新训练"""
    st.subheader("重新训练模型")

    st.markdown("""
    **训练说明：**
    - 训练将使用数据库中的所有历史比赛数据
    - 首次训练可能需要几分钟时间
    - 训练完成后将覆盖现有模型文件
    - 胜负预测模型：逻辑回归 + XGBoost
    - 进球数预测模型：XGBoost回归 + 双泊松分布
    """)

    if st.button("🔄 开始训练", key="train_btn", type="primary"):
        with st.spinner("正在训练胜负预测模型..."):
            try:
                win_loss_results = train_and_save_all_models()
                st.success("胜负预测模型训练完成！")

                for model_name, metrics in win_loss_results.items():
                    if isinstance(metrics, dict) and "error" not in metrics:
                        st.markdown(f"**{model_name}**")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("准确率", f"{metrics.get('cv_accuracy', 0):.1%}")
                        col2.metric("F1-Score", f"{metrics.get('cv_f1_macro', 0):.3f}")
                        col3.metric("对数损失", f"{metrics.get('cv_log_loss', 0):.3f}")
                    elif isinstance(metrics, dict) and "error" in metrics:
                        st.error(f"{model_name}: {metrics['error']}")
            except Exception as e:
                st.error(f"胜负预测模型训练失败: {e}")

        with st.spinner("正在训练进球数预测模型..."):
            try:
                goals_results = train_and_save_goals_models()
                st.success("进球数预测模型训练完成！")

                if isinstance(goals_results, dict) and "error" not in goals_results:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("主队进球RMSE", f"{goals_results.get('home_rmse', 0):.3f}")
                        st.metric("主队进球MAE", f"{goals_results.get('home_mae', 0):.3f}")
                    with col2:
                        st.metric("客队进球RMSE", f"{goals_results.get('away_rmse', 0):.3f}")
                        st.metric("客队进球MAE", f"{goals_results.get('away_mae', 0):.3f}")
                elif isinstance(goals_results, dict) and "error" in goals_results:
                    st.error(f"进球数模型: {goals_results['error']}")
            except Exception as e:
                st.error(f"进球数预测模型训练失败: {e}")

        # 清除session中的训练标记
        if "models_trained" in st.session_state:
            del st.session_state.models_trained
