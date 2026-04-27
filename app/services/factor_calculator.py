"""因子计算模块，实现文档中定义的所有因子体系

因子分类：
1. 进攻端因子 - 场均预期进球、射正次数等
2. 防守端因子 - 场均预期失球、被射正次数等
3. 交互因子 - A队进攻与B队防守的对比
4. 状态趋势因子 - 加权近期数据、实际积分vs预期积分
5. 交锋历史因子 - 双方近K次交锋数据
6. 赛程与情景因子 - 休息天数差、主客场标识等
7. 统计离散与稳定性因子 - 预期进球/失球的方差
8. 赛季绝对实力与底蕴因子 - 排名差值、历史排名等
9. 赛季攻防统治力因子 - 净胜球差值、胜率差值等
10. 对阵不同档位球队的表现因子 - 对阵上下半区球队的表现
11. 当前赛季战意/压力因子 - 与欧战区/降级区的分差
12. 核心球员缺阵影响因子 - 核心球员缺阵对进攻的量化影响
13. 球队疲劳指数 - 基于休息天数和赛程密度的疲劳度
"""
import sqlite3
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from app.config import RECENT_MATCHES_WINDOW, HEAD_TO_HEAD_K, FORM_WEIGHT_DECAY
from app import database as db


# 因子名称映射表，用于前端展示
FACTOR_NAMES = {
    "avg_expected_goals": "场均预期进球",
    "avg_shots_on_target": "场均射正次数",
    "avg_key_passes": "场均关键传球",
    "avg_expected_goals_conceded": "场均预期失球",
    "avg_shots_on_target_conceded": "场均被射正次数",
    "avg_pressing_intensity": "场均压迫强度",
    "attack_defense_ratio": "攻防对比因子",
    "defense_attack_ratio": "防守对比因子",
    "weighted_recent_xg": "加权近期预期进球",
    "weighted_recent_xgc": "加权近期预期失球",
    "actual_vs_expected_points": "实际积分vs预期积分差",
    "h2h_win_rate": "交锋胜率",
    "h2h_avg_goals": "交锋场均进球",
    "h2h_avg_conceded": "交锋场均失球",
    "rest_days_diff": "休息天数差",
    "is_home": "主场优势",
    "xg_variance": "预期进球方差",
    "xgc_variance": "预期失球方差",
    "ranking_diff": "联赛排名差",
    "historical_ranking_diff": "历史排名差",
    "goal_difference_diff": "净胜球差",
    "win_rate_diff": "胜率差",
    "vs_top_half_points": "对阵上半区场均积分",
    "vs_bottom_half_xg": "对阵下半区场均预期进球",
    "european_zone_gap": "与欧战区分差",
    "relegation_zone_gap": "与降级区分差",
    "key_player_absence_impact": "核心球员缺阵影响",
    "fatigue_index": "疲劳指数",
}

FACTOR_DESCRIPTIONS = {
    "avg_expected_goals": "球队近期场均预期进球数，反映进攻创造力的核心指标",
    "avg_shots_on_target": "球队近期场均射正次数，反映射门精准度",
    "avg_key_passes": "球队近期场均关键传球数，反映进攻组织能力",
    "avg_expected_goals_conceded": "球队近期场均预期失球数，反映防守稳固程度",
    "avg_shots_on_target_conceded": "球队近期场均被射正次数，反映防守压力",
    "avg_pressing_intensity": "球队近期场均压迫强度，反映防守主动性",
    "attack_defense_ratio": "主队进攻能力与客队防守能力的对比值",
    "defense_attack_ratio": "主队防守能力与客队进攻能力的对比值",
    "weighted_recent_xg": "近期预期进球的加权平均，越近的比赛权重越高",
    "weighted_recent_xgc": "近期预期失球的的加权平均，越近的比赛权重越高",
    "actual_vs_expected_points": "实际积分与预期积分的差值，正值表示表现超预期",
    "h2h_win_rate": "双方近K次交锋中的胜率",
    "h2h_avg_goals": "双方近K次交锋中的场均进球数",
    "h2h_avg_conceded": "双方近K次交锋中的场均失球数",
    "rest_days_diff": "主队与客队休息天数差，正值表示主队休息更充分",
    "is_home": "主场优势标识，1表示主场，0表示客场",
    "xg_variance": "预期进球的方差，值越大表示进攻表现越不稳定",
    "xgc_variance": "预期失球的方差，值越大表示防守表现越不稳定",
    "ranking_diff": "当前联赛排名差值，正值表示主队排名更高",
    "historical_ranking_diff": "近几个赛季平均排名差值",
    "goal_difference_diff": "净胜球差值，反映攻防综合实力的差距",
    "win_rate_diff": "胜率差值，反映赢球能力的差距",
    "vs_top_half_points": "对阵联赛上半区球队的场均积分，反映强强对话能力",
    "vs_bottom_half_xg": "对阵联赛下半区球队的场均预期进球，反映虐菜能力",
    "european_zone_gap": "与欧战区（前4或前6）的分差，正值表示在欧战区内",
    "relegation_zone_gap": "与降级区的分差，正值表示远离降级区",
    "key_player_absence_impact": "核心球员缺阵时进攻效率下降的量化值",
    "fatigue_index": "0-1的疲劳度因子，越高表示越疲劳",
}


def _get_team_recent_stats(conn: sqlite3.Connection, team_id: int,
                           limit: int = RECENT_MATCHES_WINDOW) -> List[Dict[str, Any]]:
    """获取球队近期比赛统计数据

    Args:
        conn: 数据库连接
        team_id: 球队ID
        limit: 获取最近N场比赛

    Returns:
        统计数据列表
    """
    return db.get_team_stats_history(conn, team_id, limit)


def _get_team_matches_with_result(conn: sqlite3.Connection, team_id: int,
                                  season: Optional[str] = None) -> List[Dict[str, Any]]:
    """获取球队比赛记录（含结果）

    Args:
        conn: 数据库连接
        team_id: 球队ID
        season: 赛季筛选

    Returns:
        比赛记录列表
    """
    cursor = conn.cursor()
    if season:
        cursor.execute(
            """SELECT m.*, ht.team_name as home_team_name, at.team_name as away_team_name
               FROM matches m
               JOIN teams ht ON m.home_team_id = ht.team_id
               JOIN teams at ON m.away_team_id = at.team_id
               WHERE (m.home_team_id = ? OR m.away_team_id = ?) AND m.season = ?
               ORDER BY m.match_date""",
            (team_id, team_id, season)
        )
    else:
        cursor.execute(
            """SELECT m.*, ht.team_name as home_team_name, at.team_name as away_team_name
               FROM matches m
               JOIN teams ht ON m.home_team_id = ht.team_id
               JOIN teams at ON m.away_team_id = at.team_id
               WHERE m.home_team_id = ? OR m.away_team_id = ?
               ORDER BY m.match_date""",
            (team_id, team_id)
        )
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def calc_offensive_factors(stats_list: List[Dict[str, Any]]) -> Dict[str, float]:
    """计算进攻端因子

    Args:
        stats_list: 球队近期比赛统计数据列表

    Returns:
        进攻端因子字典
    """
    if not stats_list:
        return {"avg_expected_goals": 0.0, "avg_shots_on_target": 0.0, "avg_key_passes": 0.0}

    df = pd.DataFrame(stats_list)
    return {
        "avg_expected_goals": round(float(df["expected_goals"].mean()), 3),
        "avg_shots_on_target": round(float(df["shots_on_target"].mean()), 2),
        "avg_key_passes": round(float(df["key_passes"].mean()), 2),
    }


def calc_defensive_factors(stats_list: List[Dict[str, Any]]) -> Dict[str, float]:
    """计算防守端因子

    Args:
        stats_list: 球队近期比赛统计数据列表

    Returns:
        防守端因子字典
    """
    if not stats_list:
        return {"avg_expected_goals_conceded": 0.0, "avg_shots_on_target_conceded": 0.0,
                "avg_pressing_intensity": 0.0}

    df = pd.DataFrame(stats_list)
    return {
        "avg_expected_goals_conceded": round(float(df["expected_goals_conceded"].mean()), 3),
        "avg_shots_on_target_conceded": round(float(df["shots_on_target_conceded"].mean()), 2),
        "avg_pressing_intensity": round(float(df["pressing_intensity"].mean()), 2),
    }


def calc_interaction_factors(home_offensive: Dict[str, float],
                            home_defensive: Dict[str, float],
                            away_offensive: Dict[str, float],
                            away_defensive: Dict[str, float]) -> Dict[str, float]:
    """计算交互因子（A队进攻 vs B队防守）

    Args:
        home_offensive: 主队进攻因子
        home_defensive: 主队防守因子
        away_offensive: 客队进攻因子
        away_defensive: 客队防守因子

    Returns:
        交互因子字典
    """
    # 主队进攻 vs 客队防守
    home_xg = max(home_offensive.get("avg_expected_goals", 0.01), 0.01)
    away_xgc = max(away_defensive.get("avg_expected_goals_conceded", 0.01), 0.01)
    attack_defense_ratio = round(home_xg / away_xgc, 3)

    # 主队防守 vs 客队进攻
    home_xgc = max(home_defensive.get("avg_expected_goals_conceded", 0.01), 0.01)
    away_xg = max(away_offensive.get("avg_expected_goals", 0.01), 0.01)
    defense_attack_ratio = round(away_xg / home_xgc, 3)

    return {
        "attack_defense_ratio": attack_defense_ratio,
        "defense_attack_ratio": defense_attack_ratio,
    }


def calc_form_trend_factors(stats_list: List[Dict[str, Any]],
                            matches_list: List[Dict[str, Any]],
                            team_id: int) -> Dict[str, float]:
    """计算状态趋势因子

    Args:
        stats_list: 球队近期比赛统计数据列表
        matches_list: 球队近期比赛列表
        team_id: 球队ID

    Returns:
        状态趋势因子字典
    """
    if not stats_list:
        return {"weighted_recent_xg": 0.0, "weighted_recent_xgc": 0.0,
                "actual_vs_expected_points": 0.0}

    # 加权近期预期进球（越近权重越高）
    weights = [FORM_WEIGHT_DECAY ** i for i in range(len(stats_list))]
    weights.reverse()
    total_weight = sum(weights)

    df = pd.DataFrame(stats_list)
    weighted_xg = sum(df["expected_goals"].values * weights) / total_weight
    weighted_xgc = sum(df["expected_goals_conceded"].values * weights) / total_weight

    # 实际积分 vs 预期积分
    actual_points = 0
    expected_points = 0
    if matches_list:
        for match in matches_list[:RECENT_MATCHES_WINDOW]:
            if match.get("home_goals") is None or match.get("away_goals") is None:
                continue
            is_home = match["home_team_id"] == team_id
            home_goals = match["home_goals"]
            away_goals = match["away_goals"]

            if is_home:
                team_goals, opp_goals = home_goals, away_goals
            else:
                team_goals, opp_goals = away_goals, home_goals

            # 实际积分
            if team_goals > opp_goals:
                actual_points += 3
            elif team_goals == opp_goals:
                actual_points += 1

            # 预期积分（基于预期进球的简化计算）
            team_xg = match.get("expected_goals", 1.0) if is_home else match.get("expected_goals", 0.8)
            if team_xg > opp_goals * 0.8:
                expected_points += 3
            elif abs(team_xg - opp_goals * 0.8) < 0.5:
                expected_points += 1

    return {
        "weighted_recent_xg": round(float(weighted_xg), 3),
        "weighted_recent_xgc": round(float(weighted_xgc), 3),
        "actual_vs_expected_points": round(float(actual_points - expected_points), 2),
    }


def calc_head_to_head_factors(conn: sqlite3.Connection, home_team_id: int,
                              away_team_id: int) -> Dict[str, float]:
    """计算交锋历史因子

    Args:
        conn: 数据库连接
        home_team_id: 主队ID
        away_team_id: 客队ID

    Returns:
        交锋历史因子字典
    """
    h2h_matches = db.get_head_to_head_matches(conn, home_team_id, away_team_id, HEAD_TO_HEAD_K)

    if not h2h_matches:
        return {"h2h_win_rate": 0.5, "h2h_avg_goals": 0.0, "h2h_avg_conceded": 0.0}

    wins = 0
    total_goals = 0
    total_conceded = 0

    for match in h2h_matches:
        is_home = match["home_team_id"] == home_team_id
        home_goals = match.get("home_goals", 0) or 0
        away_goals = match.get("away_goals", 0) or 0

        if is_home:
            team_goals, opp_goals = home_goals, away_goals
        else:
            team_goals, opp_goals = away_goals, home_goals

        total_goals += team_goals
        total_conceded += opp_goals

        if team_goals > opp_goals:
            wins += 1

    match_count = len(h2h_matches)
    return {
        "h2h_win_rate": round(wins / match_count, 3),
        "h2h_avg_goals": round(total_goals / match_count, 2),
        "h2h_avg_conceded": round(total_conceded / match_count, 2),
    }


def calc_schedule_factors(conn: sqlite3.Connection, home_team_id: int,
                          away_team_id: int, match_date: Optional[str] = None) -> Dict[str, float]:
    """计算赛程与情景因子

    Args:
        conn: 数据库连接
        home_team_id: 主队ID
        away_team_id: 客队ID
        match_date: 比赛日期

    Returns:
        赛程因子字典
    """
    # 主场优势
    is_home = 1.0

    # 休息天数差
    rest_days_diff = 0.0
    if match_date:
        match_dt = datetime.strptime(match_date, "%Y-%m-%d")

        cursor = conn.cursor()
        for team_id, label in [(home_team_id, "home"), (away_team_id, "away")]:
            cursor.execute(
                """SELECT MAX(match_date) as last_match FROM matches
                   WHERE (home_team_id = ? OR away_team_id = ?)
                   AND match_date < ?""",
                (team_id, team_id, match_date)
            )
            row = cursor.fetchone()
            if row and row["last_match"]:
                last_match_dt = datetime.strptime(row["last_match"], "%Y-%m-%d")
                days_rest = (match_dt - last_match_dt).days
                if label == "home":
                    home_rest = days_rest
                else:
                    away_rest = days_rest
            else:
                if label == "home":
                    home_rest = 7
                else:
                    away_rest = 7

        rest_days_diff = float(home_rest - away_rest)

    return {
        "rest_days_diff": round(rest_days_diff, 1),
        "is_home": is_home,
    }


def calc_stability_factors(stats_list: List[Dict[str, Any]]) -> Dict[str, float]:
    """计算统计离散与稳定性因子

    Args:
        stats_list: 球队近期比赛统计数据列表

    Returns:
        稳定性因子字典
    """
    if not stats_list or len(stats_list) < 2:
        return {"xg_variance": 0.0, "xgc_variance": 0.0}

    df = pd.DataFrame(stats_list)
    return {
        "xg_variance": round(float(df["expected_goals"].var()), 3),
        "xgc_variance": round(float(df["expected_goals_conceded"].var()), 3),
    }


def _calc_league_standings(conn: sqlite3.Connection, league_id: int,
                           season: str) -> List[Dict[str, Any]]:
    """计算联赛积分榜

    Args:
        conn: 数据库连接
        league_id: 联赛ID
        season: 赛季

    Returns:
        积分榜列表，按积分降序排列
    """
    cursor = conn.cursor()
    cursor.execute(
        """SELECT home_team_id, away_team_id, home_goals, away_goals, result
           FROM matches WHERE league_id = ? AND season = ?
           AND home_goals IS NOT NULL AND away_goals IS NOT NULL""",
        (league_id, season)
    )
    matches = [dict(row) for row in cursor.fetchall()]

    standings = {}
    for match in matches:
        home_id = match["home_team_id"]
        away_id = match["away_team_id"]
        home_goals = match["home_goals"]
        away_goals = match["away_goals"]

        for team_id in [home_id, away_id]:
            if team_id not in standings:
                standings[team_id] = {
                    "team_id": team_id, "points": 0, "goals_for": 0,
                    "goals_against": 0, "played": 0, "wins": 0,
                    "draws": 0, "losses": 0,
                }

        # 主队统计
        standings[home_id]["played"] += 1
        standings[home_id]["goals_for"] += home_goals
        standings[home_id]["goals_against"] += away_goals
        if home_goals > away_goals:
            standings[home_id]["points"] += 3
            standings[home_id]["wins"] += 1
        elif home_goals == away_goals:
            standings[home_id]["points"] += 1
            standings[home_id]["draws"] += 1
        else:
            standings[home_id]["losses"] += 1

        # 客队统计
        standings[away_id]["played"] += 1
        standings[away_id]["goals_for"] += away_goals
        standings[away_id]["goals_against"] += home_goals
        if away_goals > home_goals:
            standings[away_id]["points"] += 3
            standings[away_id]["wins"] += 1
        elif away_goals == home_goals:
            standings[away_id]["points"] += 1
            standings[away_id]["draws"] += 1
        else:
            standings[away_id]["losses"] += 1

    result = sorted(standings.values(), key=lambda x: (x["points"], x["goals_for"] - x["goals_against"]), reverse=True)
    for rank, team in enumerate(result, 1):
        team["rank"] = rank

    return result


def calc_strength_factors(conn: sqlite3.Connection, home_team_id: int,
                          away_team_id: int, league_id: int,
                          season: str) -> Dict[str, float]:
    """计算赛季绝对实力与底蕴因子

    Args:
        conn: 数据库连接
        home_team_id: 主队ID
        away_team_id: 客队ID
        league_id: 联赛ID
        season: 赛季

    Returns:
        实力因子字典
    """
    standings = _calc_league_standings(conn, league_id, season)

    home_rank = len(standings)
    away_rank = len(standings)
    historical_rank_diff = 0.0

    for team in standings:
        if team["team_id"] == home_team_id:
            home_rank = team["rank"]
        if team["team_id"] == away_team_id:
            away_rank = team["rank"]

    # 计算历史排名差（取近几个赛季）
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT season FROM matches WHERE league_id = ? ORDER BY season DESC", (league_id,))
    seasons_list = [row["season"] for row in cursor.fetchall()]

    historical_ranks_home = []
    historical_ranks_away = []
    for past_season in seasons_list[:3]:
        past_standings = _calc_league_standings(conn, league_id, past_season)
        for team in past_standings:
            if team["team_id"] == home_team_id:
                historical_ranks_home.append(team["rank"])
            if team["team_id"] == away_team_id:
                historical_ranks_away.append(team["rank"])

    if historical_ranks_home and historical_ranks_away:
        avg_home_rank = sum(historical_ranks_home) / len(historical_ranks_home)
        avg_away_rank = sum(historical_ranks_away) / len(historical_ranks_away)
        historical_rank_diff = round(avg_away_rank - avg_home_rank, 2)

    return {
        "ranking_diff": round(float(away_rank - home_rank), 1),
        "historical_ranking_diff": historical_rank_diff,
    }


def calc_dominance_factors(conn: sqlite3.Connection, home_team_id: int,
                           away_team_id: int, league_id: int,
                           season: str) -> Dict[str, float]:
    """计算赛季攻防统治力因子

    Args:
        conn: 数据库连接
        home_team_id: 主队ID
        away_team_id: 客队ID
        league_id: 联赛ID
        season: 赛季

    Returns:
        统治力因子字典
    """
    standings = _calc_league_standings(conn, league_id, season)

    home_gd = 0
    away_gd = 0
    home_wr = 0.0
    away_wr = 0.0

    for team in standings:
        if team["team_id"] == home_team_id:
            home_gd = team["goals_for"] - team["goals_against"]
            home_wr = team["wins"] / max(team["played"], 1)
        if team["team_id"] == away_team_id:
            away_gd = team["goals_for"] - team["goals_against"]
            away_wr = team["wins"] / max(team["played"], 1)

    return {
        "goal_difference_diff": round(float(home_gd - away_gd), 2),
        "win_rate_diff": round(float(home_wr - away_wr), 3),
    }


def calc_vs_tier_factors(conn: sqlite3.Connection, team_id: int,
                         league_id: int, season: str) -> Dict[str, float]:
    """计算对阵不同档位球队的表现因子

    Args:
        conn: 数据库连接
        team_id: 球队ID
        league_id: 联赛ID
        season: 赛季

    Returns:
        对阵档位因子字典
    """
    standings = _calc_league_standings(conn, league_id, season)
    if not standings:
        return {"vs_top_half_points": 0.0, "vs_bottom_half_xg": 0.0}

    # 确定上下半区分界
    mid_rank = len(standings) // 2
    top_half_ids = {t["team_id"] for t in standings if t["rank"] <= mid_rank}
    bottom_half_ids = {t["team_id"] for t in standings if t["rank"] > mid_rank}

    cursor = conn.cursor()
    cursor.execute(
        """SELECT m.home_team_id, m.away_team_id, m.home_goals, m.away_goals, m.result
           FROM matches m
           WHERE m.league_id = ? AND m.season = ?
           AND (m.home_team_id = ? OR m.away_team_id = ?)
           AND m.home_goals IS NOT NULL""",
        (league_id, season, team_id, team_id)
    )
    matches = [dict(row) for row in cursor.fetchall()]

    # 对阵上半区球队的积分
    top_half_points = 0
    top_half_games = 0
    # 对阵下半区球队的预期进球
    bottom_half_xg_sum = 0.0
    bottom_half_games = 0

    for match in matches:
        is_home = match["home_team_id"] == team_id
        opp_id = match["away_team_id"] if is_home else match["home_team_id"]

        if opp_id in top_half_ids:
            top_half_games += 1
            home_goals = match["home_goals"] or 0
            away_goals = match["away_goals"] or 0
            if is_home:
                team_goals, opp_goals = home_goals, away_goals
            else:
                team_goals, opp_goals = away_goals, home_goals

            if team_goals > opp_goals:
                top_half_points += 3
            elif team_goals == opp_goals:
                top_half_points += 1

        if opp_id in bottom_half_ids:
            bottom_half_games += 1
            # 获取该场比赛的预期进球
            cursor.execute(
                "SELECT expected_goals FROM match_stats WHERE match_id = ? AND team_id = ?",
                (match.get("match_id", 0), team_id)
            )
            stat_row = cursor.fetchone()
            if stat_row:
                bottom_half_xg_sum += stat_row["expected_goals"] or 0

    return {
        "vs_top_half_points": round(top_half_points / max(top_half_games, 1), 2),
        "vs_bottom_half_xg": round(bottom_half_xg_sum / max(bottom_half_games, 1), 3),
    }


def calc_motivation_factors(conn: sqlite3.Connection, team_id: int,
                            league_id: int, season: str) -> Dict[str, float]:
    """计算当前赛季战意/压力因子

    Args:
        conn: 数据库连接
        team_id: 球队ID
        league_id: 联赛ID
        season: 赛季

    Returns:
        战意因子字典
    """
    standings = _calc_league_standings(conn, league_id, season)
    if not standings:
        return {"european_zone_gap": 0.0, "relegation_zone_gap": 0.0}

    team_points = 0
    team_rank = len(standings)
    total_teams = len(standings)

    for team in standings:
        if team["team_id"] == team_id:
            team_points = team["points"]
            team_rank = team["rank"]

    # 欧战区（前4名）的分差
    european_cutoff_rank = min(4, total_teams)
    european_points = 0
    for team in standings:
        if team["rank"] == european_cutoff_rank:
            european_points = team["points"]
            break

    # 降级区（后3名）的分差
    relegation_cutoff_rank = max(total_teams - 3, total_teams // 2)
    relegation_points = 0
    for team in standings:
        if team["rank"] == relegation_cutoff_rank:
            relegation_points = team["points"]
            break

    return {
        "european_zone_gap": round(float(team_points - european_points), 1),
        "relegation_zone_gap": round(float(team_points - relegation_points), 1),
    }


def calc_player_absence_factor(conn: sqlite3.Connection, team_id: int) -> Dict[str, float]:
    """计算核心球员缺阵影响因子

    Args:
        conn: 数据库连接
        team_id: 球队ID

    Returns:
        球员缺阵因子字典
    """
    # MVP阶段使用简化计算：基于球队近期进球效率波动估算
    stats = db.get_team_stats_history(conn, team_id, 10)
    if not stats:
        return {"key_player_absence_impact": 0.0}

    df = pd.DataFrame(stats)
    xg_std = df["expected_goals"].std()
    xg_mean = df["expected_goals"].mean()

    # 用变异系数近似球员缺阵影响
    impact = 0.0
    if xg_mean > 0:
        cv = xg_std / xg_mean
        impact = min(cv * 0.5, 1.0)

    return {"key_player_absence_impact": round(float(impact), 3)}


def calc_fatigue_factor(conn: sqlite3.Connection, team_id: int,
                        match_date: Optional[str] = None) -> Dict[str, float]:
    """计算球队疲劳指数

    Args:
        conn: 数据库连接
        team_id: 球队ID
        match_date: 比赛日期

    Returns:
        疲劳因子字典
    """
    fatigue = 0.0

    if match_date:
        match_dt = datetime.strptime(match_date, "%Y-%m-%d")
        cursor = conn.cursor()

        # 最近30天内的比赛数
        thirty_days_ago = (match_dt - timedelta(days=30)).strftime("%Y-%m-%d")
        cursor.execute(
            """SELECT COUNT(*) as cnt FROM matches
               WHERE (home_team_id = ? OR away_team_id = ?)
               AND match_date >= ? AND match_date < ?""",
            (team_id, team_id, thirty_days_ago, match_date)
        )
        row = cursor.fetchone()
        recent_games = row["cnt"] if row else 0

        # 最近7天内的比赛数
        seven_days_ago = (match_dt - timedelta(days=7)).strftime("%Y-%m-%d")
        cursor.execute(
            """SELECT COUNT(*) as cnt FROM matches
               WHERE (home_team_id = ? OR away_team_id = ?)
               AND match_date >= ? AND match_date < ?""",
            (team_id, team_id, seven_days_ago, match_date)
        )
        row = cursor.fetchone()
        weekly_games = row["cnt"] if row else 0

        # 上一场比赛的休息天数
        cursor.execute(
            """SELECT MAX(match_date) as last_match FROM matches
               WHERE (home_team_id = ? OR away_team_id = ?)
               AND match_date < ?""",
            (team_id, team_id, match_date)
        )
        row = cursor.fetchone()
        rest_days = 7
        if row and row["last_match"]:
            rest_days = (match_dt - datetime.strptime(row["last_match"], "%Y-%m-%d")).days

        # 综合疲劳指数（0-1）
        monthly_load = min(recent_games / 10.0, 1.0)
        weekly_load = min(weekly_games / 3.0, 1.0)
        rest_penalty = max(0, 1.0 - rest_days / 7.0)

        fatigue = (monthly_load * 0.3 + weekly_load * 0.4 + rest_penalty * 0.3)

    return {"fatigue_index": round(min(fatigue, 1.0), 3)}


def calculate_all_factors(conn: sqlite3.Connection, home_team_id: int,
                          away_team_id: int, league_id: int,
                          season: str, match_date: Optional[str] = None) -> Dict[str, float]:
    """计算所有因子，返回完整的因子字典

    Args:
        conn: 数据库连接
        home_team_id: 主队ID
        away_team_id: 客队ID
        league_id: 联赛ID
        season: 赛季
        match_date: 比赛日期

    Returns:
        完整的因子字典
    """
    # 获取近期统计数据
    home_stats = _get_team_recent_stats(conn, home_team_id)
    away_stats = _get_team_recent_stats(conn, away_team_id)

    # 获取近期比赛记录
    home_matches = _get_team_matches_with_result(conn, home_team_id, season)
    away_matches = _get_team_matches_with_result(conn, away_team_id, season)

    # 1. 进攻端因子
    home_offensive = calc_offensive_factors(home_stats)
    away_offensive = calc_offensive_factors(away_stats)

    # 2. 防守端因子
    home_defensive = calc_defensive_factors(home_stats)
    away_defensive = calc_defensive_factors(away_stats)

    # 3. 交互因子
    interaction = calc_interaction_factors(home_offensive, home_defensive,
                                           away_offensive, away_defensive)

    # 4. 状态趋势因子
    home_form = calc_form_trend_factors(home_stats, home_matches, home_team_id)
    away_form = calc_form_trend_factors(away_stats, away_matches, away_team_id)

    # 5. 交锋历史因子
    h2h = calc_head_to_head_factors(conn, home_team_id, away_team_id)

    # 6. 赛程与情景因子
    schedule = calc_schedule_factors(conn, home_team_id, away_team_id, match_date)

    # 7. 稳定性因子
    home_stability = calc_stability_factors(home_stats)
    away_stability = calc_stability_factors(away_stats)

    # 8. 实力因子
    strength = calc_strength_factors(conn, home_team_id, away_team_id, league_id, season)

    # 9. 统治力因子
    dominance = calc_dominance_factors(conn, home_team_id, away_team_id, league_id, season)

    # 10. 对阵档位因子
    home_vs_tier = calc_vs_tier_factors(conn, home_team_id, league_id, season)
    away_vs_tier = calc_vs_tier_factors(conn, away_team_id, league_id, season)

    # 11. 战意因子
    home_motivation = calc_motivation_factors(conn, home_team_id, league_id, season)
    away_motivation = calc_motivation_factors(conn, away_team_id, league_id, season)

    # 12. 核心球员缺阵因子
    home_absence = calc_player_absence_factor(conn, home_team_id)
    away_absence = calc_player_absence_factor(conn, away_team_id)

    # 13. 疲劳因子
    home_fatigue = calc_fatigue_factor(conn, home_team_id, match_date)
    away_fatigue = calc_fatigue_factor(conn, away_team_id, match_date)

    # 合并所有因子，区分主客队
    all_factors = {}

    # 主队进攻因子
    for key, value in home_offensive.items():
        all_factors[f"home_{key}"] = value

    # 客队进攻因子
    for key, value in away_offensive.items():
        all_factors[f"away_{key}"] = value

    # 主队防守因子
    for key, value in home_defensive.items():
        all_factors[f"home_{key}"] = value

    # 客队防守因子
    for key, value in away_defensive.items():
        all_factors[f"away_{key}"] = value

    # 交互因子
    all_factors.update(interaction)

    # 主队状态趋势
    for key, value in home_form.items():
        all_factors[f"home_{key}"] = value

    # 客队状态趋势
    for key, value in away_form.items():
        all_factors[f"away_{key}"] = value

    # 交锋历史
    all_factors.update(h2h)

    # 赛程因子
    all_factors.update(schedule)

    # 主队稳定性
    for key, value in home_stability.items():
        all_factors[f"home_{key}"] = value

    # 客队稳定性
    for key, value in away_stability.items():
        all_factors[f"away_{key}"] = value

    # 实力因子
    all_factors.update(strength)

    # 统治力因子
    all_factors.update(dominance)

    # 主队对阵档位
    for key, value in home_vs_tier.items():
        all_factors[f"home_{key}"] = value

    # 客队对阵档位
    for key, value in away_vs_tier.items():
        all_factors[f"away_{key}"] = value

    # 主队战意
    for key, value in home_motivation.items():
        all_factors[f"home_{key}"] = value

    # 客队战意
    for key, value in away_motivation.items():
        all_factors[f"away_{key}"] = value

    # 主队球员缺阵
    all_factors["home_key_player_absence_impact"] = home_absence["key_player_absence_impact"]
    all_factors["away_key_player_absence_impact"] = away_absence["key_player_absence_impact"]

    # 疲劳因子
    all_factors["home_fatigue_index"] = home_fatigue["fatigue_index"]
    all_factors["away_fatigue_index"] = away_fatigue["fatigue_index"]

    return all_factors


def get_factor_display_info(factor_key: str) -> Dict[str, str]:
    """获取因子的展示信息（名称和描述）

    Args:
        factor_key: 因子键名

    Returns:
        包含name和description的字典
    """
    # 去除home_/away_前缀查找
    base_key = factor_key
    for prefix in ["home_", "away_"]:
        if factor_key.startswith(prefix):
            base_key = factor_key[len(prefix):]
            break

    name = FACTOR_NAMES.get(base_key, factor_key)
    description = FACTOR_DESCRIPTIONS.get(base_key, "")

    # 添加主/客队前缀
    if factor_key.startswith("home_"):
        name = f"主队{name}"
    elif factor_key.startswith("away_"):
        name = f"客队{name}"

    return {"name": name, "description": description}
