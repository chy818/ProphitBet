"""数据管理页面 - 手动录入比赛、球队数据，API数据刷新，因子调整"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
from datetime import date

from app.database import get_connection, get_all_teams, get_all_leagues, get_db
from app.database import (insert_factor_adjustment, get_active_factor_adjustments,
                          get_all_factor_adjustments, update_factor_adjustment,
                          deactivate_factor_adjustment, delete_factor_adjustment)
from app.services.data_collector import fetch_external_data
from app.services.football_data_collector import fetch_football_data, fetch_historical_data
from app.services.factor_calculator import (FACTOR_NAMES, FACTOR_CATEGORIES,
                                            ADJUSTABLE_FACTORS, calculate_all_factors)

st.set_page_config(page_title="数据管理", page_icon="⚙️", layout="wide")

st.title("⚙️ 数据管理")
st.markdown("手动录入比赛数据、管理球队信息、刷新API数据、手动调整因子")

# 标签页
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["⚽ 录入比赛", "🏟️ 管理球队", "🔄 API数据刷新", "📊 数据概览", "🎯 因子调整"]
)

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

        # 因子调整统计
        st.markdown("### 因子调整统计")
        cursor.execute("SELECT COUNT(*) as cnt FROM factor_adjustments WHERE is_active = 1")
        active_adjustments = cursor.fetchone()["cnt"]
        cursor.execute("SELECT COUNT(*) as cnt FROM factor_adjustments")
        total_adjustments = cursor.fetchone()["cnt"]
        col5, col6 = st.columns(2)
        col5.metric("生效中的调整", active_adjustments)
        col6.metric("总调整记录", total_adjustments)

    with tab5:
        """因子手动调整"""
        st.subheader("🎯 因子手动调整")
        st.markdown("""
        > **说明**：手动调整因子值可以覆盖系统计算的结果。
        > 支持设置生效时间范围，可用于模拟球员缺阵、战术调整等场景。
        > 调整后的因子会影响后续的预测结果。
        """)

        # 子标签页：添加调整 / 查看调整 / 管理调整 / 因子说明 / 球队因子值
        adj_tab1, adj_tab2, adj_tab3, adj_tab4, adj_tab5 = st.tabs(
            ["➕ 添加调整", "📋 当前生效", "🗂️ 全部记录", "📖 因子说明", "📊 球队因子值"]
        )

        with adj_tab1:
            """添加新的因子调整"""
            st.markdown("#### 选择球队")
            leagues_for_adj = get_all_leagues(conn)
            league_options_for_adj = {lg["league_name"]: lg["league_id"] for lg in leagues_for_adj}
            selected_league_for_adj = st.selectbox("选择联赛", list(league_options_for_adj.keys()),
                                                   key="adj_league_select")
            selected_league_id_for_adj = league_options_for_adj[selected_league_for_adj]

            teams_for_adj = get_all_teams(conn, selected_league_id_for_adj)
            if not teams_for_adj:
                st.warning("该联赛暂无球队数据")
            else:
                team_options_for_adj = {t["team_name"]: t["team_id"] for t in teams_for_adj}
                selected_team_name_for_adj = st.selectbox("选择球队", list(team_options_for_adj.keys()),
                                                          key="adj_team_select")
                selected_team_id_for_adj = team_options_for_adj[selected_team_name_for_adj]

                st.markdown("#### 选择参考对手（用于计算原始因子值）")
                opponent_options = {k: v for k, v in team_options_for_adj.items() if v != selected_team_id_for_adj}
                if opponent_options:
                    default_opponent = list(opponent_options.keys())[0]
                    selected_opponent_name = st.selectbox("选择对手球队", list(opponent_options.keys()),
                                                         index=list(opponent_options.keys()).index(default_opponent) if default_opponent in opponent_options else 0,
                                                         key="adj_opponent_select")
                    selected_opponent_id_for_adj = opponent_options[selected_opponent_name]

                    st.markdown("#### 选择要调整的因子")
                    category_options = ["全部"] + list(set(FACTOR_CATEGORIES.values()))
                    selected_category = st.selectbox("按分类筛选", category_options, key="adj_category_select")

                    available_factors = []
                    for factor in ADJUSTABLE_FACTORS:
                        cat = FACTOR_CATEGORIES.get(factor, "其他")
                        if selected_category == "全部" or cat == selected_category:
                            display_name = FACTOR_NAMES.get(factor, factor)
                            available_factors.append((factor, f"[{cat}] {display_name}"))

                    factor_options = {k: v for k, v in available_factors}
                    selected_factor = st.selectbox("选择因子", list(factor_options.keys()),
                                                  format_func=lambda x: factor_options[x],
                                                  key="adj_factor_select")

                    st.markdown("#### 设置调整值")

                    with get_db() as db_conn:
                        factors = calculate_all_factors(
                            db_conn, selected_team_id_for_adj, selected_opponent_id_for_adj,
                            selected_league_id_for_adj, "2025-2026", None
                        )
                        home_factor_key = f"home_{selected_factor}"
                        original_value = factors.get(home_factor_key, 0.0) if home_factor_key in factors else 0.0

                    col_adj1, col_adj2 = st.columns(2)
                    with col_adj1:
                        st.number_input("原始值（参考）", value=float(original_value), format="%.3f",
                                        key="adj_original_value", disabled=True)
                        adjusted_value = st.number_input("调整后的值", value=float(original_value), format="%.3f",
                                                         key="adj_adjusted_value")

                    with col_adj2:
                        reason = st.text_area("调整原因", placeholder="例如：核心前锋伤缺，进攻能力下降",
                                              key="adj_reason")
                        effective_from = st.date_input("生效日期（留空则立即生效）",
                                                        value=None, key="adj_effective_from")
                        effective_to = st.date_input("失效日期（留空则永久生效）",
                                                      value=None, key="adj_effective_to")

                    if st.button("✅ 添加调整", type="primary", use_container_width=True):
                        try:
                            with get_db() as db_conn:
                                insert_factor_adjustment(
                                    db_conn,
                                    team_id=selected_team_id_for_adj,
                                    factor_name=selected_factor,
                                    factor_category=FACTOR_CATEGORIES.get(selected_factor, "其他"),
                                    adjusted_value=adjusted_value,
                                    original_value=original_value,
                                    reason=reason if reason else None,
                                    effective_from=str(effective_from) if effective_from else None,
                                    effective_to=str(effective_to) if effective_to else None
                                )
                            st.success(f"添加成功！球队「{selected_team_name_for_adj}」的「{FACTOR_NAMES.get(selected_factor, selected_factor)}」已调整为 {adjusted_value}")
                        except Exception as e:
                            st.error(f"添加失败: {e}")
                else:
                    st.warning("该联赛至少需要2支球队才能计算因子")

        with adj_tab2:
            """查看当前生效的因子调整"""
            st.markdown("#### 当前生效的因子调整")
            active_adjustments = get_active_factor_adjustments(conn)

            if active_adjustments:
                df_active = pd.DataFrame(active_adjustments)
                display_cols = ["team_name", "factor_category", "factor_name",
                                "adjusted_value", "original_value", "reason",
                                "effective_from", "effective_to"]
                df_display = df_active[display_cols].copy()
                df_display.columns = ["球队", "分类", "因子", "调整值", "原始值", "原因",
                                      "生效日期", "失效日期"]
                st.dataframe(df_display, use_container_width=True, hide_index=True)

                st.markdown("#### 停用调整")
                adj_to_deactivate = st.selectbox(
                    "选择要停用的调整",
                    [(a["adjustment_id"], f"{a['team_name']} - {FACTOR_NAMES.get(a['factor_name'], a['factor_name'])}")
                     for a in active_adjustments],
                    format_func=lambda x: x[1],
                    key="deactivate_select"
                )
                if st.button("⛔ 停用选中调整", use_container_width=True):
                    try:
                        with get_db() as db_conn:
                            deactivate_factor_adjustment(db_conn, adj_to_deactivate[0])
                        st.success("停用成功！")
                        st.rerun()
                    except Exception as e:
                        st.error(f"停用失败: {e}")
            else:
                st.info("暂无生效中的因子调整")

        with adj_tab3:
            """查看全部调整记录"""
            st.markdown("#### 全部调整记录")
            all_adjustments = get_all_factor_adjustments(conn)

            if all_adjustments:
                df_all = pd.DataFrame(all_adjustments)
                display_cols = ["team_name", "factor_category", "factor_name",
                                "adjusted_value", "original_value", "reason",
                                "is_active", "effective_from", "effective_to", "created_at"]
                df_display = df_all[display_cols].copy()
                df_display.columns = ["球队", "分类", "因子", "调整值", "原始值", "原因",
                                      "是否生效", "生效日期", "失效日期", "创建时间"]
                df_display["是否生效"] = df_display["是否生效"].apply(lambda x: "是" if x else "否")
                st.dataframe(df_display, use_container_width=True, hide_index=True)

                st.markdown("#### 删除调整记录")
                adj_to_delete = st.selectbox(
                    "选择要删除的调整",
                    [(a["adjustment_id"], f"{a['team_name']} - {FACTOR_NAMES.get(a['factor_name'], a['factor_name'])}")
                     for a in all_adjustments],
                    format_func=lambda x: x[1],
                    key="delete_select"
                )
                col_del1, col_del2 = st.columns(2)
                with col_del1:
                    if st.button("🗑️ 删除选中记录", use_container_width=True):
                        try:
                            with get_db() as db_conn:
                                delete_factor_adjustment(db_conn, adj_to_delete[0])
                            st.success("删除成功！")
                            st.rerun()
                        except Exception as e:
                            st.error(f"删除失败: {e}")
                with col_del2:
                    if st.button("⛔ 停用选中记录", use_container_width=True):
                        try:
                            with get_db() as db_conn:
                                deactivate_factor_adjustment(db_conn, adj_to_delete[0])
                            st.success("停用成功！")
                            st.rerun()
                        except Exception as e:
                            st.error(f"停用失败: {e}")
            else:
                st.info("暂无调整记录")

        with adj_tab4:
            """因子说明文档"""
            st.markdown("#### 📖 因子计算原理说明")

            from app.services.factor_calculator import FACTOR_PRINCIPLES

            category_order = ["进攻端", "防守端", "交互", "状态趋势", "交锋历史",
                            "赛程情景", "稳定性", "实力", "统治力", "对阵档位",
                            "战意", "球员缺阵", "疲劳"]

            for category in category_order:
                factors_in_category = [
                    (name, info) for name, info in FACTOR_PRINCIPLES.items()
                    if info["category"] == category
                ]
                if factors_in_category:
                    with st.expander(f"**{category}**（{len(factors_in_category)}个因子）", expanded=False):
                        for factor_name, info in factors_in_category:
                            display_name = FACTOR_NAMES.get(factor_name, factor_name)
                            st.markdown(f"""
                            ---
                            ### {display_name}
                            - **因子名称**: `{factor_name}`
                            - **数据来源**: {info['data_source']}
                            - **计算方式**: {info['calculation']}
                            - **计算公式**: `{info['formula']}`
                            - **权重因子**: {info['weight_factor']}
                            - **计算示例**: {info['example']}
                            """)

            st.markdown("---")
            st.markdown("""
            ### 📊 数据完整性说明

            | 数据类型 | 数据来源 | 可靠性 |
            |---------|---------|--------|
            | 比赛结果（进球数） | football-data.org | ✅ 真实 |
            | 比赛日期 | football-data.org | ✅ 真实 |
            | 预期进球(xG)等统计 | 模拟生成 | ⚠️ 基于球队实力的估算 |
            | 射门、关键传球等 | 模拟生成 | ⚠️ 基于球队实力的估算 |
            | 交锋历史 | football-data.org | ✅ 真实（但受样本量影响） |
            | 休息天数、赛程 | 基于比赛日期计算 | ✅ 真实 |
            | 积分榜排名 | football-data.org | ✅ 真实 |
            """)

        with adj_tab5:
            """球队因子值展示"""
            st.markdown("#### 📊 球队因子值查询")
            st.markdown("""
            > **说明**：选择球队和参考对手后，系统会计算并展示该球队的所有因子值。
            > 因子值基于球队近期比赛统计数据计算得出。
            """)

            # 选择联赛
            st.markdown("##### 选择联赛和球队")
            leagues_for_factors = get_all_leagues(conn)
            league_options_for_factors = {lg["league_name"]: lg["league_id"] for lg in leagues_for_factors}
            selected_league_for_factors = st.selectbox("选择联赛", list(league_options_for_factors.keys()),
                                                       key="factors_league_select")
            selected_league_id_for_factors = league_options_for_factors[selected_league_for_factors]

            # 选择球队
            teams_for_factors = get_all_teams(conn, selected_league_id_for_factors)
            if not teams_for_factors:
                st.warning("该联赛暂无球队数据")
            else:
                team_options_for_factors = {t["team_name"]: t["team_id"] for t in teams_for_factors}
                col_f1, col_f2 = st.columns(2)
                with col_f1:
                    selected_team_for_factors = st.selectbox("选择球队", list(team_options_for_factors.keys()),
                                                             key="factors_team_select")
                    selected_team_id_for_factors = team_options_for_factors[selected_team_for_factors]

                with col_f2:
                    opponent_options_for_factors = {k: v for k, v in team_options_for_factors.items()
                                                    if v != selected_team_id_for_factors}
                    if opponent_options_for_factors:
                        selected_opponent_for_factors = st.selectbox("选择参考对手",
                                                                     list(opponent_options_for_factors.keys()),
                                                                     key="factors_opponent_select")
                        selected_opponent_id_for_factors = opponent_options_for_factors[selected_opponent_for_factors]
                    else:
                        st.warning("该联赛至少需要2支球队才能计算因子")
                        selected_opponent_id_for_factors = None

                if selected_opponent_id_for_factors and st.button("🔍 计算因子值", type="primary"):
                    with st.spinner("正在计算因子值..."):
                        with get_db() as db_conn:
                            factors = calculate_all_factors(
                                db_conn, selected_team_id_for_factors, selected_opponent_id_for_factors,
                                selected_league_id_for_factors, "2025-2026", None
                            )

                    # 按分类整理因子值
                    st.markdown(f"##### {selected_team_for_factors} vs {selected_opponent_for_factors} 因子值")

                    category_data = {}
                    for key, value in factors.items():
                        if key.startswith("home_"):
                            factor_name = key.replace("home_", "")
                            category = FACTOR_CATEGORIES.get(factor_name, "其他")
                            if category not in category_data:
                                category_data[category] = []
                            display_name = FACTOR_NAMES.get(factor_name, factor_name)
                            category_data[category].append({
                                "因子": display_name,
                                "英文名": factor_name,
                                "值": round(value, 4) if isinstance(value, float) else value
                            })

                    # 按分类展示
                    category_order = ["进攻端", "防守端", "交互", "状态趋势", "交锋历史",
                                    "赛程情景", "稳定性", "实力", "统治力", "对阵档位",
                                    "战意", "球员缺阵", "疲劳"]

                    for category in category_order:
                        if category in category_data and category_data[category]:
                            with st.expander(f"**{category}**", expanded=(category in ["进攻端", "防守端"])):
                                df = pd.DataFrame(category_data[category])
                                st.dataframe(df, use_container_width=True, hide_index=True)

                    # 导出功能
                    st.markdown("---")
                    st.markdown("##### 导出因子值")
                    all_factors_df = pd.DataFrame([
                        {"分类": FACTOR_CATEGORIES.get(k.replace("home_", ""), "其他"),
                         "因子": FACTOR_NAMES.get(k.replace("home_", ""), k),
                         "英文名": k.replace("home_", ""),
                         "值": round(v, 4) if isinstance(v, float) else v}
                        for k, v in factors.items() if k.startswith("home_")
                    ])
                    csv = all_factors_df.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        label="📥 导出CSV",
                        data=csv,
                        file_name=f"{selected_team_for_factors}_factors.csv",
                        mime="text/csv"
                    )

finally:
    conn.close()
