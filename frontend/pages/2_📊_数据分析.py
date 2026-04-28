"""数据分析页面 - 球队数据对比、因子分析、联赛积分榜"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from app.database import get_connection, get_all_teams, get_all_leagues, init_database
from app.services.data_cleaner import clean_and_prepare_data
from app.services.factor_calculator import (
    calc_offensive_factors, calc_defensive_factors,
    calc_stability_factors, _get_team_recent_stats,
    _calc_league_standings,
)

st.set_page_config(page_title="数据分析", page_icon="📊", layout="wide")

# 确保数据库已初始化（包括新建的factor_switches表）
init_database()

st.title("📊 数据分析")
st.markdown("查看球队数据对比、因子分析和联赛积分榜")


# 标签页
tab1, tab2, tab3 = st.tabs(["🏆 联赛积分榜", "⚔️ 球队对比", "📈 数据概览"])

conn = get_connection()

try:
    with tab1:
        """联赛积分榜"""
        st.subheader("联赛积分榜")

        leagues = get_all_leagues(conn)
        league_options = {lg["league_name"]: lg["league_id"] for lg in leagues}
        selected_league = st.selectbox("选择联赛", list(league_options.keys()), key="standings_league")
        league_id = league_options[selected_league]

        # 赛季选择
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT season FROM matches WHERE league_id = ? ORDER BY season DESC", (league_id,))
        seasons = [row["season"] for row in cursor.fetchall()]
        if seasons:
            selected_season = st.selectbox("选择赛季", seasons, index=0, key="standings_season")
        else:
            st.warning("暂无赛季数据")
            selected_season = None

        if selected_season:
            standings = _calc_league_standings(conn, league_id, selected_season)

            if standings:
                # 构建积分榜DataFrame
                standings_data = []
                for team in standings:
                    cursor.execute("SELECT team_name FROM teams WHERE team_id = ?", (team["team_id"],))
                    row = cursor.fetchone()
                    team_name = row["team_name"] if row else "未知"

                    gd = team["goals_for"] - team["goals_against"]
                    standings_data.append({
                        "排名": team["rank"],
                        "球队": team_name,
                        "场次": team["played"],
                        "胜": team["wins"],
                        "平": team.get("draws", 0),
                        "负": team.get("losses", 0),
                        "进球": team["goals_for"],
                        "失球": team["goals_against"],
                        "净胜球": gd,
                        "积分": team["points"],
                    })

                df_standings = pd.DataFrame(standings_data)

                # 使用样式显示积分榜
                st.dataframe(
                    df_standings.style.apply(
                        lambda x: ["background-color: #e8f5e9" if i < 4
                                   else "background-color: #fff3e0" if 4 <= i < 6
                                   else "background-color: #ffebee" if i >= len(df_standings) - 3
                                   else "" for i in range(len(df_standings))],
                        axis=0, subset=["排名"]
                    ),
                    use_container_width=True,
                    height=600,
                )

                # 积分分布图
                fig = go.Figure(data=[go.Bar(
                    x=df_standings["球队"],
                    y=df_standings["积分"],
                    marker_color=df_standings["排名"].apply(
                        lambda x: "#27ae60" if x <= 4 else "#f39c12" if x <= 6 else "#e74c3c" if x > len(df_standings) - 3 else "#3498db"
                    ),
                )])
                fig.update_layout(
                    title=f"{selected_league} {selected_season} 积分分布",
                    xaxis_title="球队",
                    yaxis_title="积分",
                    height=400,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无积分榜数据")

    with tab2:
        """球队对比"""
        st.subheader("球队数据对比")

        leagues = get_all_leagues(conn)
        league_options = {lg["league_name"]: lg["league_id"] for lg in leagues}
        selected_league = st.selectbox("选择联赛", list(league_options.keys()), key="compare_league")
        league_id = league_options[selected_league]

        teams = get_all_teams(conn, league_id)
        team_options = {t["team_name"]: t["team_id"] for t in teams}

        col1, col2 = st.columns(2)
        with col1:
            team_a_name = st.selectbox("球队A", list(team_options.keys()), index=0, key="team_a")
            team_a_id = team_options[team_a_name]
        with col2:
            team_b_name = st.selectbox("球队B", list(team_options.keys()), index=min(1, len(team_options) - 1), key="team_b")
            team_b_id = team_options[team_b_name]

        if st.button("📊 对比分析", key="compare_btn"):
            # 获取两队近期统计
            stats_a = _get_team_recent_stats(conn, team_a_id, 10)
            stats_b = _get_team_recent_stats(conn, team_b_id, 10)

            if stats_a and stats_b:
                # 计算因子
                offensive_a = calc_offensive_factors(stats_a)
                offensive_b = calc_offensive_factors(stats_b)
                defensive_a = calc_defensive_factors(stats_a)
                defensive_b = calc_defensive_factors(stats_b)
                stability_a = calc_stability_factors(stats_a)
                stability_b = calc_stability_factors(stats_b)

                # 雷达图对比
                categories = ["预期进球", "射正", "关键传球", "压迫强度", "稳定性"]
                values_a = [
                    offensive_a.get("avg_expected_goals", 0),
                    offensive_a.get("avg_shots_on_target", 0) / 10,
                    offensive_a.get("avg_key_passes", 0) / 15,
                    defensive_a.get("avg_pressing_intensity", 0) / 200,
                    1 - stability_a.get("xg_variance", 0),
                ]
                values_b = [
                    offensive_b.get("avg_expected_goals", 0),
                    offensive_b.get("avg_shots_on_target", 0) / 10,
                    offensive_b.get("avg_key_passes", 0) / 15,
                    defensive_b.get("avg_pressing_intensity", 0) / 200,
                    1 - stability_b.get("xg_variance", 0),
                ]

                fig_radar = go.Figure()
                fig_radar.add_trace(go.Scatterpolar(
                    r=values_a + [values_a[0]],
                    theta=categories + [categories[0]],
                    fill="toself",
                    name=team_a_name,
                    line_color="#27ae60",
                ))
                fig_radar.add_trace(go.Scatterpolar(
                    r=values_b + [values_b[0]],
                    theta=categories + [categories[0]],
                    fill="toself",
                    name=team_b_name,
                    line_color="#e74c3c",
                ))
                fig_radar.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, max(max(values_a), max(values_b)) * 1.2])),
                    title="球队综合能力对比",
                    height=500,
                )
                st.plotly_chart(fig_radar, use_container_width=True)

                # 详细数据对比表
                compare_data = {
                    "指标": ["场均预期进球", "场均射正", "场均关键传球", "场均预期失球", "场均被射正", "场均压迫强度", "预期进球方差", "预期失球方差"],
                    team_a_name: [
                        offensive_a.get("avg_expected_goals", 0),
                        offensive_a.get("avg_shots_on_target", 0),
                        offensive_a.get("avg_key_passes", 0),
                        defensive_a.get("avg_expected_goals_conceded", 0),
                        defensive_a.get("avg_shots_on_target_conceded", 0),
                        defensive_a.get("avg_pressing_intensity", 0),
                        stability_a.get("xg_variance", 0),
                        stability_a.get("xgc_variance", 0),
                    ],
                    team_b_name: [
                        offensive_b.get("avg_expected_goals", 0),
                        offensive_b.get("avg_shots_on_target", 0),
                        offensive_b.get("avg_key_passes", 0),
                        defensive_b.get("avg_expected_goals_conceded", 0),
                        defensive_b.get("avg_shots_on_target_conceded", 0),
                        defensive_b.get("avg_pressing_intensity", 0),
                        stability_b.get("xg_variance", 0),
                        stability_b.get("xgc_variance", 0),
                    ],
                }
                st.dataframe(pd.DataFrame(compare_data), use_container_width=True)
            else:
                st.warning("暂无足够数据进行对比")

    with tab3:
        """数据概览"""
        st.subheader("数据概览")

        data = clean_and_prepare_data(conn)
        matches_df = data["matches"]
        stats_df = data["stats"]

        if not matches_df.empty:
            st.markdown(f"**比赛数据**: {len(matches_df)} 条记录")

            # 比赛结果分布
            if "result" in matches_df.columns:
                result_counts = matches_df["result"].value_counts()
                fig_result = go.Figure(data=[go.Pie(
                    labels=result_counts.index.tolist(),
                    values=result_counts.values.tolist(),
                    marker=dict(colors=["#27ae60", "#f39c12", "#e74c3c"]),
                    hole=0.4,
                )])
                fig_result.update_layout(title="比赛结果分布", height=400)
                st.plotly_chart(fig_result, use_container_width=True)

            # 进球分布
            if "home_goals" in matches_df.columns and "away_goals" in matches_df.columns:
                total_goals = matches_df["home_goals"].dropna() + matches_df["away_goals"].dropna()
                fig_goals = go.Figure(data=[go.Histogram(x=total_goals, nbinsx=10, marker_color="#3498db")])
                fig_goals.update_layout(
                    title="总进球数分布",
                    xaxis_title="总进球数",
                    yaxis_title="比赛场次",
                    height=400,
                )
                st.plotly_chart(fig_goals, use_container_width=True)

        if not stats_df.empty:
            st.markdown(f"**统计数据**: {len(stats_df)} 条记录")

            # 预期进球分布
            if "expected_goals" in stats_df.columns:
                fig_xg = go.Figure(data=[go.Histogram(x=stats_df["expected_goals"].dropna(), nbinsx=20, marker_color="#9b59b6")])
                fig_xg.update_layout(
                    title="预期进球(xG)分布",
                    xaxis_title="预期进球",
                    yaxis_title="频次",
                    height=400,
                )
                st.plotly_chart(fig_xg, use_container_width=True)

finally:
    conn.close()
