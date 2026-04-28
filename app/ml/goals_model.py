"""进球数预测模型模块，实现双泊松分布和XGBoost回归两种方法"""
import os
import math
from typing import Dict, Any, Optional, Tuple, List

import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error
import xgboost as xgb
import joblib

from app.config import MODEL_DIR, CROSS_VALIDATION_FOLDS, RANDOM_SEED
from app.services.factor_calculator import calculate_all_factors, get_enabled_goals_feature_columns
from app import database as db


# 默认进球数模型特征列（所有因子启用时使用）
DEFAULT_GOALS_FEATURE_COLUMNS = [
    "home_avg_expected_goals", "home_avg_shots_on_target", "home_avg_key_passes",
    "away_avg_expected_goals_conceded", "away_avg_shots_on_target_conceded",
    "attack_defense_ratio",
    "home_weighted_recent_xg", "away_weighted_recent_xgc",
    "h2h_avg_goals",
    "home_xg_variance", "away_xgc_variance",
    "home_vs_bottom_half_xg",
    "home_key_player_absence_impact",
    "home_fatigue_index", "away_fatigue_index",
    "is_home", "rest_days_diff",
    "ranking_diff", "goal_difference_diff",
]

DEFAULT_CONCEDED_FEATURE_COLUMNS = [
    "away_avg_expected_goals", "away_avg_shots_on_target", "away_avg_key_passes",
    "home_avg_expected_goals_conceded", "home_avg_shots_on_target_conceded",
    "defense_attack_ratio",
    "away_weighted_recent_xg", "home_weighted_recent_xgc",
    "h2h_avg_conceded",
    "away_xg_variance", "home_xgc_variance",
    "away_vs_bottom_half_xg",
    "away_key_player_absence_impact",
    "home_fatigue_index", "away_fatigue_index",
    "is_home", "rest_days_diff",
    "ranking_diff", "goal_difference_diff",
]

# 兼容旧代码的别名
GOALS_FEATURE_COLUMNS = DEFAULT_GOALS_FEATURE_COLUMNS
CONCEDED_FEATURE_COLUMNS = DEFAULT_CONCEDED_FEATURE_COLUMNS


def prepare_goals_training_data(conn=None,
                                 home_features: Optional[List[str]] = None,
                                 away_features: Optional[List[str]] = None) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    """准备进球数模型的训练数据

    Args:
        conn: 数据库连接
        home_features: 主队进球特征列，None则根据启用的因子动态生成
        away_features: 客队进球特征列，None则根据启用的因子动态生成

    Returns:
        (特征DataFrame, 主队进球Series, 客队进球Series)的元组
    """
    if conn is None:
        conn = db.get_connection()

    # 动态获取启用的特征列
    if home_features is None or away_features is None:
        try:
            dyn_home, dyn_away = get_enabled_goals_feature_columns(conn)
            if home_features is None:
                home_features = dyn_home
            if away_features is None:
                away_features = dyn_away
        except Exception:
            home_features = home_features or DEFAULT_GOALS_FEATURE_COLUMNS
            away_features = away_features or DEFAULT_CONCEDED_FEATURE_COLUMNS

    if not home_features:
        home_features = DEFAULT_GOALS_FEATURE_COLUMNS
    if not away_features:
        away_features = DEFAULT_CONCEDED_FEATURE_COLUMNS

    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT m.match_id, m.home_team_id, m.away_team_id, m.league_id,
                      m.season, m.match_date, m.home_goals, m.away_goals
               FROM matches m
               WHERE m.home_goals IS NOT NULL AND m.away_goals IS NOT NULL
               ORDER BY m.match_date"""
        )
        columns = [desc[0] for desc in cursor.description]
        matches = [dict(zip(columns, row)) for row in cursor.fetchall()]

        if not matches:
            return pd.DataFrame(), pd.Series(), pd.Series()

        home_features_list = []
        away_features_list = []
        home_goals_list = []
        away_goals_list = []

        for match in matches:
            try:
                factors = calculate_all_factors(
                    conn, match["home_team_id"], match["away_team_id"],
                    match["league_id"], match["season"], match["match_date"]
                )

                home_feat = {col: factors.get(col, 0.0) for col in home_features}
                away_feat = {col: factors.get(col, 0.0) for col in away_features}

                home_features_list.append(home_feat)
                away_features_list.append(away_feat)
                home_goals_list.append(match["home_goals"])
                away_goals_list.append(match["away_goals"])
            except Exception:
                continue

        if not home_features_list:
            return pd.DataFrame(), pd.Series(), pd.Series()

        # 合并主客队特征
        X_home = pd.DataFrame(home_features_list).fillna(0.0)
        X_away = pd.DataFrame(away_features_list).fillna(0.0)

        # 拼接特征
        X = pd.concat([X_home, X_away], axis=1)
        X.columns = [f"home_{col}" if i < len(home_features) else f"away_{col}"
                     for i, col in enumerate(home_features + away_features)]

        y_home = pd.Series(home_goals_list)
        y_away = pd.Series(away_goals_list)

        return X, y_home, y_away
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def train_xgboost_goals(X: pd.DataFrame, y_home: pd.Series,
                        y_away: pd.Series) -> Tuple[Any, Any, Dict[str, Any]]:
    """训练XGBoost进球数回归模型

    Args:
        X: 特征DataFrame
        y_home: 主队进球Series
        y_away: 客队进球Series

    Returns:
        (主队模型, 客队模型, 评估指标)的元组
    """
    tscv = TimeSeriesSplit(n_splits=CROSS_VALIDATION_FOLDS)

    # 主队进球模型
    home_model = xgb.XGBRegressor(
        n_estimators=150,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=RANDOM_SEED,
        objective="reg:squarederror",
    )

    # 客队进球模型
    away_model = xgb.XGBRegressor(
        n_estimators=150,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=RANDOM_SEED,
        objective="reg:squarederror",
    )

    # 交叉验证评估
    home_cv_scores = []
    away_cv_scores = []

    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_home_train, y_home_val = y_home.iloc[train_idx], y_home.iloc[val_idx]
        y_away_train, y_away_val = y_away.iloc[train_idx], y_away.iloc[val_idx]

        home_model.fit(X_train, y_home_train, verbose=False)
        away_model.fit(X_train, y_away_train, verbose=False)

        home_pred = home_model.predict(X_val)
        away_pred = away_model.predict(X_val)

        home_cv_scores.append({
            "rmse": np.sqrt(mean_squared_error(y_home_val, home_pred)),
            "mae": mean_absolute_error(y_home_val, home_pred),
        })
        away_cv_scores.append({
            "rmse": np.sqrt(mean_squared_error(y_away_val, away_pred)),
            "mae": mean_absolute_error(y_away_val, away_pred),
        })

    # 全量训练
    home_model.fit(X, y_home, verbose=False)
    away_model.fit(X, y_away, verbose=False)

    metrics = {
        "model_type": "xgboost_goals",
        "home_rmse": np.mean([s["rmse"] for s in home_cv_scores]),
        "home_mae": np.mean([s["mae"] for s in home_cv_scores]),
        "away_rmse": np.mean([s["rmse"] for s in away_cv_scores]),
        "away_mae": np.mean([s["mae"] for s in away_cv_scores]),
    }

    return home_model, away_model, metrics


def predict_double_poisson(home_expected_goals: float,
                           away_expected_goals: float,
                           max_goals: int = 7) -> Dict[str, Any]:
    """使用双泊松分布计算比分概率矩阵

    Args:
        home_expected_goals: 主队预期进球数
        away_expected_goals: 客队预期进球数
        max_goals: 最大进球数

    Returns:
        包含比分概率矩阵和各结果概率的字典
    """
    # 构建比分概率矩阵
    score_matrix = np.zeros((max_goals + 1, max_goals + 1))
    for i in range(max_goals + 1):
        for j in range(max_goals + 1):
            score_matrix[i][j] = poisson.pmf(i, home_expected_goals) * poisson.pmf(j, away_expected_goals)

    # 计算胜负概率
    home_win_prob = 0.0
    draw_prob = 0.0
    away_win_prob = 0.0

    for i in range(max_goals + 1):
        for j in range(max_goals + 1):
            if i > j:
                home_win_prob += score_matrix[i][j]
            elif i == j:
                draw_prob += score_matrix[i][j]
            else:
                away_win_prob += score_matrix[i][j]

    # 最可能比分
    max_idx = np.unravel_index(np.argmax(score_matrix), score_matrix.shape)
    most_likely_score = f"{max_idx[0]}:{max_idx[1]}"

    # 大于2.5球概率
    over_25_prob = 0.0
    for i in range(max_goals + 1):
        for j in range(max_goals + 1):
            if i + j > 2.5:
                over_25_prob += score_matrix[i][j]

    # 两队都进球概率
    both_score_prob = 0.0
    for i in range(1, max_goals + 1):
        for j in range(1, max_goals + 1):
            both_score_prob += score_matrix[i][j]

    # Top5比分概率
    score_probs = []
    flat_indices = np.argsort(score_matrix.flatten())[::-1]
    for idx in flat_indices[:5]:
        i, j = divmod(idx, max_goals + 1)
        score_probs.append({
            "score": f"{i}:{j}",
            "probability": round(float(score_matrix[i][j]), 4),
        })

    return {
        "home_win_prob": round(float(home_win_prob), 4),
        "draw_prob": round(float(draw_prob), 4),
        "away_win_prob": round(float(away_win_prob), 4),
        "most_likely_score": most_likely_score,
        "over_25_prob": round(float(over_25_prob), 4),
        "both_teams_score_prob": round(float(both_score_prob), 4),
        "score_probabilities": score_probs,
        "score_matrix": score_matrix,
    }


def save_goals_model(home_model: Any, away_model: Any,
                     model_name: str = "goals_xgboost",
                     home_features: Optional[List[str]] = None,
                     away_features: Optional[List[str]] = None) -> str:
    """保存进球数模型

    Args:
        home_model: 主队进球模型
        away_model: 客队进球模型
        model_name: 模型名称
        home_features: 主队特征列名
        away_features: 客队特征列名

    Returns:
        保存路径
    """
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, f"{model_name}.joblib")
    save_data = {
        "home_model": home_model,
        "away_model": away_model,
        "home_features": home_features or DEFAULT_GOALS_FEATURE_COLUMNS,
        "away_features": away_features or DEFAULT_CONCEDED_FEATURE_COLUMNS,
    }
    joblib.dump(save_data, model_path)
    return model_path


def load_goals_model(model_name: str = "goals_xgboost") -> Tuple[Any, Any, List[str], List[str]]:
    """加载进球数模型

    Args:
        model_name: 模型名称

    Returns:
        (主队模型, 客队模型, 主队特征列, 客队特征列)的元组
    """
    model_path = os.path.join(MODEL_DIR, f"{model_name}.joblib")
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"模型文件不存在: {model_path}")

    save_data = joblib.load(model_path)
    return (save_data["home_model"], save_data["away_model"],
            save_data.get("home_features", DEFAULT_GOALS_FEATURE_COLUMNS),
            save_data.get("away_features", DEFAULT_CONCEDED_FEATURE_COLUMNS))


def train_and_save_goals_models() -> Dict[str, Any]:
    """训练并保存所有进球数模型

    Returns:
        训练结果摘要
    """
    conn = db.get_connection()

    # 获取启用的特征列
    try:
        home_features, away_features = get_enabled_goals_feature_columns(conn)
    except Exception:
        home_features = DEFAULT_GOALS_FEATURE_COLUMNS
        away_features = DEFAULT_CONCEDED_FEATURE_COLUMNS

    if not home_features:
        home_features = DEFAULT_GOALS_FEATURE_COLUMNS
    if not away_features:
        away_features = DEFAULT_CONCEDED_FEATURE_COLUMNS

    try:
        X, y_home, y_away = prepare_goals_training_data(conn, home_features, away_features)
    except Exception:
        conn.close()
        return {"error": "训练数据准备失败"}

    if X.empty or y_home.empty:
        return {"error": "训练数据为空，请先生成示例数据"}

    home_model, away_model, metrics = train_xgboost_goals(X, y_home, y_away)
    save_goals_model(home_model, away_model,
                     home_features=home_features,
                     away_features=away_features)

    metrics["enabled_home_features"] = len(home_features)
    metrics["enabled_away_features"] = len(away_features)

    return metrics
