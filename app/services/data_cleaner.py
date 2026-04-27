"""数据清洗模块，负责处理缺失值、异常值，确保数据质量"""
import sqlite3
from typing import Dict, Any, List, Optional

import numpy as np
import pandas as pd

from app import database as db


def load_matches_as_dataframe(conn: sqlite3.Connection,
                              league_id: Optional[int] = None) -> pd.DataFrame:
    """从数据库加载比赛数据为DataFrame

    Args:
        conn: 数据库连接
        league_id: 可选的联赛ID筛选

    Returns:
        包含比赛数据的DataFrame
    """
    matches = []
    if league_id:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT m.*, ht.team_name as home_team_name, at.team_name as away_team_name
               FROM matches m
               JOIN teams ht ON m.home_team_id = ht.team_id
               JOIN teams at ON m.away_team_id = at.team_id
               WHERE m.league_id = ?
               ORDER BY m.match_date""",
            (league_id,)
        )
    else:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT m.*, ht.team_name as home_team_name, at.team_name as away_team_name
               FROM matches m
               JOIN teams ht ON m.home_team_id = ht.team_id
               JOIN teams at ON m.away_team_id = at.team_id
               ORDER BY m.match_date"""
        )

    columns = [desc[0] for desc in cursor.description]
    for row in cursor.fetchall():
        matches.append(dict(zip(columns, row)))

    return pd.DataFrame(matches) if matches else pd.DataFrame()


def load_stats_as_dataframe(conn: sqlite3.Connection) -> pd.DataFrame:
    """从数据库加载比赛统计数据为DataFrame

    Args:
        conn: 数据库连接

    Returns:
        包含统计数据的DataFrame
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM match_stats")
    columns = [desc[0] for desc in cursor.description]
    stats = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return pd.DataFrame(stats) if stats else pd.DataFrame()


def clean_missing_values(df: pd.DataFrame,
                         strategy: str = "median") -> pd.DataFrame:
    """处理缺失值

    Args:
        df: 原始DataFrame
        strategy: 填充策略，可选mean/median/mode/zero

    Returns:
        清洗后的DataFrame
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    for col in numeric_cols:
        if df[col].isnull().any():
            if strategy == "mean":
                fill_value = df[col].mean()
            elif strategy == "median":
                fill_value = df[col].median()
            elif strategy == "mode":
                fill_value = df[col].mode().iloc[0] if not df[col].mode().empty else 0
            elif strategy == "zero":
                fill_value = 0
            else:
                fill_value = df[col].median()

            df[col] = df[col].fillna(fill_value)

    return df


def detect_and_handle_outliers(df: pd.DataFrame,
                               columns: Optional[List[str]] = None,
                               method: str = "iqr",
                               threshold: float = 3.0) -> pd.DataFrame:
    """检测并处理异常值

    Args:
        df: 原始DataFrame
        columns: 需要检测的列名列表，None则检测所有数值列
        method: 检测方法，iqr或zscore
        threshold: zscore方法的阈值

    Returns:
        处理异常值后的DataFrame
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()

    for col in columns:
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            continue

        if method == "iqr":
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            df[col] = df[col].clip(lower_bound, upper_bound)
        elif method == "zscore":
            mean_val = df[col].mean()
            std_val = df[col].std()
            if std_val > 0:
                z_scores = np.abs((df[col] - mean_val) / std_val)
                outlier_mask = z_scores > threshold
                df.loc[outlier_mask, col] = mean_val

    return df


def validate_match_data(df: pd.DataFrame) -> Dict[str, Any]:
    """验证比赛数据的完整性和一致性

    Args:
        df: 比赛数据DataFrame

    Returns:
        验证结果字典，包含是否通过和问题列表
    """
    issues = []

    if df.empty:
        issues.append("数据为空")
        return {"valid": False, "issues": issues}

    # 检查必要字段是否存在
    required_columns = ["match_id", "home_team_id", "away_team_id", "match_date"]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        issues.append(f"缺少必要字段: {missing_cols}")

    # 检查进球数是否为非负整数
    if "home_goals" in df.columns:
        invalid_home = df[df["home_goals"] < 0]
        if not invalid_home.empty:
            issues.append(f"主队进球数存在负值: {len(invalid_home)}条")

    if "away_goals" in df.columns:
        invalid_away = df[df["away_goals"] < 0]
        if not invalid_away.empty:
            issues.append(f"客队进球数存在负值: {len(invalid_away)}条")

    # 检查主客队是否相同
    if "home_team_id" in df.columns and "away_team_id" in df.columns:
        same_team = df[df["home_team_id"] == df["away_team_id"]]
        if not same_team.empty:
            issues.append(f"存在主客队相同的比赛: {len(same_team)}条")

    return {"valid": len(issues) == 0, "issues": issues}


def clean_and_prepare_data(conn: sqlite3.Connection,
                           league_id: Optional[int] = None) -> Dict[str, pd.DataFrame]:
    """完整的数据清洗流程，返回清洗后的比赛数据和统计数据

    Args:
        conn: 数据库连接
        league_id: 可选的联赛ID筛选

    Returns:
        包含matches和stats两个DataFrame的字典
    """
    # 加载原始数据
    matches_df = load_matches_as_dataframe(conn, league_id)
    stats_df = load_stats_as_dataframe(conn)

    # 验证数据
    validation = validate_match_data(matches_df)
    if not validation["valid"]:
        print(f"数据验证发现问题: {validation['issues']}")

    # 清洗缺失值
    if not matches_df.empty:
        matches_df = clean_missing_values(matches_df, strategy="median")
    if not stats_df.empty:
        stats_df = clean_missing_values(stats_df, strategy="median")

    # 处理异常值
    outlier_columns = [
        "expected_goals", "shots_on_target", "key_passes", "shots",
        "expected_goals_conceded", "shots_on_target_conceded",
        "pressing_intensity", "aerial_duel_success", "goalkeeper_save_rate"
    ]
    existing_cols = [c for c in outlier_columns if c in stats_df.columns]
    if existing_cols:
        stats_df = detect_and_handle_outliers(stats_df, columns=existing_cols, method="iqr")

    return {"matches": matches_df, "stats": stats_df}
