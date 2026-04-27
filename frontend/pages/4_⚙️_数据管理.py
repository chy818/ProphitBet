"""数据管理页面 - 手动录入比赛、球队数据，API数据刷新"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd

from app.database import get_connection, get_all_teams, get_all_leagues, get_db
from app.services.data_collector import fetch_external_data
from app.services.football_data_collector import fetch_football_data, fetch_historical_data

st.set_page_config(page_title="数据管理", page_icon="⚙️", layout="wide")

st.title("⚙️ 数据管理")
st.markdown("手动录入比赛数据、管理球队信息、刷新API数据")

# 标签页
tab1, tab2, tab3, tab4 = st.tabs(["⚽ 录入比赛", "🏟️ 管理球队", "🔄 API数据刷新", "📊 数据概览"])

conn = get_connection()

try:
    with tab1:
        """录入比赛"""
        st.subheader("录入比赛结果")

        leagues = get_all_leagues(conn)
        league_options = {lg["league_name"]: lg["league_id"] for lg in leagues}

        # 选择联赛
        selected_league_name = st.selectbox("选择联赛", list(league_options.keys()), key="input_league")
        selected_league_id = league_options[selected_league_name]

        # 获取该联赛的球队
        teams = get_all_teams(conn, selected_league_id)
        team_options = {t["team_name"]: t["team_id"] for t in teams}

        if not team_options:
            st.warning("该联赛暂无球队数据，请先在「管理球队」中添加球队")
        else:
            # 比赛信息输入
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("### 🏠 主队")
                home_team_name = st.selectbox("主队", list(team_options.keys()), key="input_home_team")
                home_team_id = team_options[home_team_name]
                home_goals = st.number_input("主队进球", min_value=0, max_value=20, value=0, key="input_home_goals")

            with col2:
                st.markdown("### ✈️ 客队")
                away_teams = {k: v for k, v in team_options.items() if v != home_team_id}
                if not away_teams:
                    st.warning("请选择不同的球队")
                    away_team_name = None
                    away_team_id = None
                else:
                    away_team_name = st.selectbox("客队", list(away_teams.keys()), key="input_away_team")
                    away_team_id = away_teams[away_team_name]
                    away_goals = st.number_input("客队进球", min_value=0, max_value=20, value=0, key="input_away_goals")

            # 比赛日期和赛季
            col3, col4 = st.columns(2)
            with col3:
                match_date = st.date_input("比赛日期", key="input_date")
            with col4:
                # 根据日期自动计算赛季
                if match_date.month >= 8:
                    default_season = f"{match_date.year}-{match_date.year + 1}"
                else:
                    default_season = f"{match_date.year - 1}-{match_date.year}"
                season = st.text_input("赛季", value=default_season, key="input_season")

            # 提交按钮
            if st.button("✅ 录入比赛", type="primary", use_container_width=True, disabled=(away_team_id is None)):
                if home_team_id == away_team_id:
                    st.error("主队和客队不能相同")
                else:
                    # 确定比赛结果
                    if home_goals > away_goals:
                        result = "主胜"
                    elif home_goals == away_goals:
                        result = "平局"
                    else:
                        result = "客胜"

                    try:
                        with get_db() as db_conn:
                            from app import database as db_module
                            db_module.insert_match(
                                db_conn,
                                home_team_id, away_team_id,
                                str(match_date), selected_league_id,
                                season, home_goals, away_goals, result
                            )
                        st.success(f"录入成功: {home_team_name} {home_goals}-{away_goals} {away_team_name} ({result})")
                    except Exception as e:
                        st.error(f"录入失败: {e}")

        # 最近录入的比赛
        st.markdown("---")
        st.subheader("最近录入的比赛")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT m.match_date, l.league_name, m.season,
                   ht.team_name as home, m.home_goals, m.away_goals, at.team_name as away, m.result
            FROM matches m
            JOIN leagues l ON m.league_id = l.league_id
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            ORDER BY m.match_id DESC
            LIMIT 20
        """)
        recent_matches = cursor.fetchall()
        if recent_matches:
            df = pd.DataFrame([dict(r) for r in recent_matches])
            df.columns = ["日期", "联赛", "赛季", "主队", "主队进球", "客队进球", "客队", "结果"]
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("暂无比赛数据")

    with tab2:
        """管理球队"""
        st.subheader("管理球队")

        # 添加新球队
        with st.expander("➕ 添加新球队", expanded=False):
            add_league_name = st.selectbox("所属联赛", list(league_options.keys()), key="add_team_league")
            add_league_id = league_options[add_league_name]

            col_a, col_b, col_c = st.columns(3)
            with col_a:
                new_team_name = st.text_input("球队名称", key="add_team_name")
            with col_b:
                new_home_ground = st.text_input("主场", key="add_team_ground")
            with col_c:
                new_founded_year = st.number_input("成立年份", min_value=1800, max_value=2026, value=1900, key="add_team_year")

            if st.button("添加球队", key="add_team_btn"):
                if not new_team_name.strip():
                    st.error("请输入球队名称")
                else:
                    try:
                        with get_db() as db_conn:
                            from app import database as db_module
                            db_module.insert_team(db_conn, new_team_name.strip(), add_league_id, new_home_ground, new_founded_year)
                        st.success(f"球队「{new_team_name}」添加成功")
                        st.rerun()
                    except Exception as e:
                        st.error(f"添加失败: {e}")

        # 球队列表
        st.markdown("### 球队列表")
        view_league_name = st.selectbox("筛选联赛", ["全部"] + list(league_options.keys()), key="view_team_league")
        if view_league_name == "全部":
            view_league_id = None
        else:
            view_league_id = league_options[view_league_name]

        teams_list = get_all_teams(conn, view_league_id)
        if teams_list:
            df_teams = pd.DataFrame(teams_list)
            # 获取联赛名称映射
            league_name_map = {lg["league_id"]: lg["league_name"] for lg in leagues}
            df_teams["联赛"] = df_teams["league_id"].map(league_name_map)
            display_cols = ["team_id", "team_name", "联赛", "home_ground", "founded_year"]
            display_names = ["ID", "球队名称", "联赛", "主场", "成立年份"]
            df_display = df_teams[display_cols].copy()
            df_display.columns = display_names
            st.dataframe(df_display, use_container_width=True, hide_index=True)
        else:
            st.info("暂无球队数据")

    with tab3:
        """API数据刷新"""
        st.subheader("API数据刷新")

        # 数据源选择
        data_source = st.radio(
            "选择数据源",
            ["football-data.org（推荐）", "聚合数据（Juhe）"],
            key="data_source",
            help="football-data.org 提供更完整的历史数据，推荐使用"
        )

        if data_source.startswith("football-data"):
            # football-data.org 数据源
            from app.config import FOOTBALL_DATA_API_TOKEN
            if FOOTBALL_DATA_API_TOKEN:
                st.success("API Token 已配置")
            else:
                st.error("API Token 未配置，请在 .env 文件中设置 FOOTBALL_DATA_API_TOKEN")

            st.markdown("### 获取当前赛季数据")
            st.markdown("获取五大联赛当前赛季的积分榜和已完赛比赛数据")

            if st.button("🔄 获取当前赛季数据", type="primary",
                         use_container_width=True, disabled=(not FOOTBALL_DATA_API_TOKEN)):
                with st.spinner("正在从 football-data.org 获取数据，请稍候..."):
                    try:
                        stats = fetch_football_data()
                        st.success(
                            f"获取完成！联赛: {stats['leagues']}, "
                            f"新球队: {stats['teams']}, 新比赛: {stats['matches']}"
                        )
                        if stats["errors"]:
                            for err in stats["errors"]:
                                st.warning(err)
                    except Exception as e:
                        st.error(f"获取失败: {e}")

            st.markdown("### 获取历史赛季数据")
            st.markdown("获取过去N个赛季的完整比赛数据（模型训练需要大量历史数据）")

            seasons_back = st.number_input(
                "获取过去几个赛季", min_value=1, max_value=10, value=3,
                key="fd_seasons_back",
                help="football-data.org 支持查询历史赛季，建议获取3个赛季"
            )

            if st.button("📚 获取历史数据", use_container_width=True,
                         disabled=(not FOOTBALL_DATA_API_TOKEN)):
                with st.spinner(f"正在获取过去 {seasons_back} 个赛季数据，可能需要几分钟..."):
                    try:
                        stats = fetch_historical_data(seasons_back=int(seasons_back))
                        st.success(
                            f"获取完成！联赛: {stats['leagues']}, "
                            f"新球队: {stats['teams']}, 新比赛: {stats['matches']}"
                        )
                        if stats["errors"]:
                            for err in stats["errors"]:
                                st.warning(err)
                    except Exception as e:
                        st.error(f"获取失败: {e}")

            st.markdown("---")
            st.markdown("### football-data.org API说明")
            st.markdown("""
            - **数据源**: football-data.org（欧洲足球数据权威来源）
            - **优势**: 支持历史赛季查询，数据完整准确
            - **覆盖**: 英超、西甲、德甲、意甲、法甲等10+联赛
            - **限制**: 免费版每分钟10次请求
            - **推荐**: 先获取历史数据（3个赛季），再定期获取当前赛季数据
            """)

        else:
            # 聚合数据源
            from app.config import JUHE_API_KEY
            if JUHE_API_KEY:
                st.success("API密钥已配置")
            else:
                st.error("API密钥未配置，请在 .env 文件中设置 JUHE_API_KEY")

            st.markdown("### 选择刷新范围")
            refresh_options = {
                "全部联赛": list(league_options.keys()),
                "仅五大联赛": ["英超", "西甲", "德甲", "意甲", "法甲"],
            }
            refresh_scope = st.radio("刷新范围", list(refresh_options.keys()), key="refresh_scope")

            target_leagues = refresh_options[refresh_scope]
            st.markdown(f"将刷新以下联赛: {', '.join(target_leagues)}")

            if st.button("🔄 开始刷新", type="primary",
                         use_container_width=True, disabled=(not JUHE_API_KEY)):
                with st.spinner("正在从API获取数据，请稍候..."):
                    try:
                        fetch_external_data()
                        st.success("数据刷新完成！")
                    except Exception as e:
                        st.error(f"刷新失败: {e}")

            st.markdown("---")
            st.markdown("### 聚合数据API说明")
            st.markdown("""
            - **接口提供商**: 聚合数据（Juhe）
            - **接口1**: 联赛积分榜 - 获取各联赛当前赛季的球队排名和积分
            - **接口2**: 近期赛程 - 获取各联赛近期已完赛和未开赛的比赛
            - **限制**: 每日请求次数有限，请合理使用
            - **注意**: API只提供近期数据，历史数据需要手动录入
            """)

    with tab4:
        """数据概览"""
        st.subheader("数据概览")

        cursor = conn.cursor()

        # 总体统计
        cursor.execute("SELECT COUNT(*) as cnt FROM teams")
        team_count = cursor.fetchone()["cnt"]
        cursor.execute("SELECT COUNT(*) as cnt FROM matches")
        match_count = cursor.fetchone()["cnt"]
        cursor.execute("SELECT COUNT(DISTINCT season) as cnt FROM matches")
        season_count = cursor.fetchone()["cnt"]
        cursor.execute("SELECT COUNT(DISTINCT league_id) as cnt FROM leagues")
        league_count = cursor.fetchone()["cnt"]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("球队总数", team_count)
        col2.metric("比赛总数", match_count)
        col3.metric("联赛数量", league_count)
        col4.metric("赛季数量", season_count)

        # 各联赛统计
        st.markdown("### 各联赛统计")
        cursor.execute("""
            SELECT l.league_name,
                   (SELECT COUNT(*) FROM teams t WHERE t.league_id = l.league_id) as team_count,
                   (SELECT COUNT(*) FROM matches m WHERE m.league_id = l.league_id) as match_count
            FROM leagues l
            ORDER BY l.league_id
        """)
        league_stats = cursor.fetchall()
        if league_stats:
            df_league = pd.DataFrame([dict(r) for r in league_stats])
            df_league.columns = ["联赛", "球队数", "比赛数"]
            st.dataframe(df_league, use_container_width=True, hide_index=True)

        # 赛季统计
        st.markdown("### 赛季统计")
        cursor.execute("""
            SELECT season, COUNT(*) as cnt
            FROM matches
            GROUP BY season
            ORDER BY season
        """)
        season_stats = cursor.fetchall()
        if season_stats:
            df_season = pd.DataFrame([dict(r) for r in season_stats])
            df_season.columns = ["赛季", "比赛数"]
            st.dataframe(df_season, use_container_width=True, hide_index=True)

finally:
    conn.close()
