"""预测页面 - 用户选择比赛并查看预测结果"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from app.database import get_connection, get_all_teams, get_all_leagues, init_database
from app.services.prediction_service import predict_match
from app.services.factor_calculator import get_factor_display_info

st.set_page_config(page_title="比赛预测", page_icon="🔮", layout="wide")

# 确保数据库已初始化（包括新建的factor_switches表）
init_database()

st.title("🔮 比赛预测")
st.markdown("选择比赛，获取基于因子化模型的概率预测")


def get_current_season():
    """获取当前赛季字符串"""
    from datetime import datetime
    now = datetime.now()
    if now.month >= 8:
        return f"{now.year}-{now.year + 1}"
    else:
        return f"{now.year - 1}-{now.year}"


# 侧边栏 - 比赛选择
st.sidebar.header("比赛选择")

conn = get_connection()
try:
    # 联赛选择
    leagues = get_all_leagues(conn)
    league_options = {lg["league_name"]: lg["league_id"] for lg in leagues}
    selected_league_name = st.sidebar.selectbox("选择联赛", list(league_options.keys()))
    selected_league_id = league_options[selected_league_name]

    # 获取该联赛的球队
    teams = get_all_teams(conn, selected_league_id)
    team_options = {t["team_name"]: t["team_id"] for t in teams}

    # 主队选择
    home_team_name = st.sidebar.selectbox("选择主队", list(team_options.keys()), index=0)
    home_team_id = team_options[home_team_name]

    # 客队选择（排除主队）
    away_teams = {k: v for k, v in team_options.items() if v != home_team_id}
    away_team_name = st.sidebar.selectbox("选择客队", list(away_teams.keys()), index=0)
    away_team_id = away_teams[away_team_name]

    # 模型选择
    model_type = st.sidebar.selectbox("预测模型", ["xgboost", "logistic"], index=0)

    # 预测按钮
    predict_button = st.sidebar.button("🚀 开始预测", use_container_width=True, type="primary")

    if predict_button:
        season = get_current_season()

        with st.spinner("正在计算因子和预测..."):
            try:
                result = predict_match(
                    home_team_id=home_team_id,
                    away_team_id=away_team_id,
                    league_id=selected_league_id,
                    season=season,
                    model_type=model_type,
                )

                st.session_state.prediction_result = result
            except Exception as e:
                st.error(f"预测失败: {e}")

finally:
    conn.close()

# 显示预测结果
if "prediction_result" in st.session_state:
    result = st.session_state.prediction_result

    # 比赛标题
    st.markdown(f"## {result['home_team_name']} 🆚 {result['away_team_name']}")
    st.markdown("---")

    # 第一行：胜负概率
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            f"<div class='metric-card'><h3>🏠 主胜</h3>"
            f"<p class='prob-home' style='font-size:2.5rem;'>{result['home_win_prob']:.1%}</p></div>",
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"<div class='metric-card'><h3>🤝 平局</h3>"
            f"<p class='prob-draw' style='font-size:2.5rem;'>{result['draw_prob']:.1%}</p></div>",
            unsafe_allow_html=True
        )

    with col3:
        st.markdown(
            f"<div class='metric-card'><h3>✈️ 客胜</h3>"
            f"<p class='prob-away' style='font-size:2.5rem;'>{result['away_win_prob']:.1%}</p></div>",
            unsafe_allow_html=True
        )

    # 第二行：核心指标
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("主队预期进球", f"{result['home_expected_goals']:.2f}")
    with col2:
        st.metric("客队预期进球", f"{result['away_expected_goals']:.2f}")
    with col3:
        st.metric("最可能比分", result['most_likely_score'])
    with col4:
        st.metric("大于2.5球概率", f"{result.get('over_25_prob', 0):.1%}")

    st.markdown("---")

    # 第三行：图表
    col1, col2 = st.columns(2)

    with col1:
        # 胜负概率饼图
        fig_pie = go.Figure(data=[go.Pie(
            labels=["主胜", "平局", "客胜"],
            values=[result['home_win_prob'], result['draw_prob'], result['away_win_prob']],
            marker=dict(colors=["#27ae60", "#f39c12", "#e74c3c"]),
            hole=0.4,
            textinfo="label+percent",
            textfont=dict(size=14),
        )])
        fig_pie.update_layout(
            title="胜负概率分布",
            height=400,
            showlegend=True,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # 比分概率柱状图
        score_probs = result.get("score_probabilities", [])
        if score_probs:
            fig_bar = go.Figure(data=[go.Bar(
                x=[s["score"] for s in score_probs],
                y=[s["probability"] for s in score_probs],
                marker_color="#3498db",
                text=[f"{s['probability']:.1%}" for s in score_probs],
                textposition="auto",
            )])
            fig_bar.update_layout(
                title="Top5 最可能比分",
                xaxis_title="比分",
                yaxis_title="概率",
                height=400,
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    # 第四行：额外概率指标
    st.markdown("### 📈 详细概率指标")
    detail_col1, detail_col2, detail_col3 = st.columns(3)

    with detail_col1:
        # 大小球概率
        over_prob = result.get("over_25_prob", 0)
        under_prob = 1 - over_prob
        fig_ou = go.Figure(data=[go.Pie(
            labels=[f"大于{2.5}球", f"小于{2.5}球"],
            values=[over_prob, under_prob],
            marker=dict(colors=["#e74c3c", "#3498db"]),
            hole=0.4,
            textinfo="label+percent",
        )])
        fig_ou.update_layout(title="大小球概率", height=300)
        st.plotly_chart(fig_ou, use_container_width=True)

    with detail_col2:
        # 两队都进球概率
        btts_prob = result.get("both_teams_score_prob", 0)
        no_btts_prob = 1 - btts_prob
        fig_btts = go.Figure(data=[go.Pie(
            labels=["两队都进球", "至少一队未进球"],
            values=[btts_prob, no_btts_prob],
            marker=dict(colors=["#27ae60", "#95a5a6"]),
            hole=0.4,
            textinfo="label+percent",
        )])
        fig_btts.update_layout(title="两队都进球概率", height=300)
        st.plotly_chart(fig_btts, use_container_width=True)

    with detail_col3:
        # 预期进球对比
        fig_xg = go.Figure(data=[go.Bar(
            x=[result["home_team_name"], result["away_team_name"]],
            y=[result["home_expected_goals"], result["away_expected_goals"]],
            marker_color=["#27ae60", "#e74c3c"],
            text=[f"{result['home_expected_goals']:.2f}", f"{result['away_expected_goals']:.2f}"],
            textposition="auto",
        )])
        fig_xg.update_layout(
            title="预期进球对比",
            yaxis_title="预期进球",
            height=300,
        )
        st.plotly_chart(fig_xg, use_container_width=True)

    # 因子分析
    st.markdown("---")
    st.markdown("### 🔬 因子分析")

    factor_details = result.get("factor_details", [])
    if factor_details:
        # 分类展示因子
        factor_categories = {
            "进攻端": ["avg_expected_goals", "avg_shots_on_target", "avg_key_passes"],
            "防守端": ["avg_expected_goals_conceded", "avg_shots_on_target_conceded", "avg_pressing_intensity"],
            "交互": ["attack_defense_ratio", "defense_attack_ratio"],
            "状态趋势": ["weighted_recent_xg", "weighted_recent_xgc", "actual_vs_expected_points"],
            "交锋历史": ["h2h_win_rate", "h2h_avg_goals", "h2h_avg_conceded"],
            "赛程情景": ["rest_days_diff", "is_home"],
            "稳定性": ["xg_variance", "xgc_variance"],
            "实力底蕴": ["ranking_diff", "historical_ranking_diff"],
            "统治力": ["goal_difference_diff", "win_rate_diff"],
            "对阵档位": ["vs_top_half_points", "vs_bottom_half_xg"],
            "战意压力": ["european_zone_gap", "relegation_zone_gap"],
            "球员缺阵": ["key_player_absence_impact"],
            "疲劳指数": ["fatigue_index"],
        }

        with st.expander("查看因子详情", expanded=False):
            for category, base_keys in factor_categories.items():
                st.markdown(f"**{category}因子**")
                cols = st.columns(min(len(base_keys) * 2, 4))
                col_idx = 0
                for base_key in base_keys:
                    for prefix in ["home_", "away_"]:
                        full_key = f"{prefix}{base_key}"
                        matching = [f for f in factor_details if f["key"] == full_key]
                        if matching:
                            factor = matching[0]
                            with cols[col_idx % len(cols)]:
                                st.metric(
                                    label=factor["name"],
                                    value=f"{factor['value']:.3f}",
                                    help=factor["description"],
                                )
                            col_idx += 1
                st.markdown("")
else:
    # 未选择比赛时的提示
    st.info("👈 请在左侧选择联赛和球队，然后点击「开始预测」")
