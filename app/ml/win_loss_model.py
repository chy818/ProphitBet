"""胜负预测模型模块，实现逻辑回归和XGBoost两种模型"""
import os
from typing import Dict, Any, Optional, Tuple, List

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, f1_score, log_loss, classification_report
from sklearn.model_selection import TimeSeriesSplit
import xgboost as xgb
import joblib

from app.config import MODEL_DIR, CROSS_VALIDATION_FOLDS, RANDOM_SEED, TRAIN_TEST_SPLIT_RATIO
from app.services.factor_calculator import calculate_all_factors, get_enabled_feature_columns
from app import database as db


# 默认模型特征列（所有因子启用时使用）
DEFAULT_FEATURE_COLUMNS = [
    "home_avg_expected_goals", "home_avg_shots_on_target", "home_avg_key_passes",
    "away_avg_expected_goals", "away_avg_shots_on_target", "away_avg_key_passes",
    "home_avg_expected_goals_conceded", "home_avg_shots_on_target_conceded",
    "away_avg_expected_goals_conceded", "away_avg_shots_on_target_conceded",
    "attack_defense_ratio", "defense_attack_ratio",
    "home_weighted_recent_xg", "home_weighted_recent_xgc",
    "away_weighted_recent_xg", "away_weighted_recent_xgc",
    "home_actual_vs_expected_points", "away_actual_vs_expected_points",
    "h2h_win_rate", "h2h_avg_goals", "h2h_avg_conceded",
    "rest_days_diff", "is_home",
    "home_xg_variance", "home_xgc_variance",
    "away_xg_variance", "away_xgc_variance",
    "ranking_diff", "historical_ranking_diff",
    "goal_difference_diff", "win_rate_diff",
    "home_vs_top_half_points", "home_vs_bottom_half_xg",
    "away_vs_top_half_points", "away_vs_bottom_half_xg",
    "home_european_zone_gap", "home_relegation_zone_gap",
    "away_european_zone_gap", "away_relegation_zone_gap",
    "home_key_player_absence_impact", "away_key_player_absence_impact",
    "home_fatigue_index", "away_fatigue_index",
]

# 兼容旧代码的别名
FEATURE_COLUMNS = DEFAULT_FEATURE_COLUMNS

# 结果编码映射
RESULT_MAP = {"主胜": 0, "平局": 1, "客胜": 2}
RESULT_MAP_INV = {0: "主胜", 1: "平局", 2: "客胜"}


def prepare_training_data(conn=None, feature_columns: Optional[List[str]] = None) -> Tuple[pd.DataFrame, pd.Series]:
    """准备模型训练数据，从数据库提取历史比赛并计算因子

    Args:
        conn: 数据库连接，None则自动获取
        feature_columns: 使用的特征列，None则根据启用的因子动态生成

    Returns:
        (特征DataFrame, 目标Series)的元组
    """
    if conn is None:
        conn = db.get_connection()

    # 动态获取启用的特征列
    if feature_columns is None:
        try:
            feature_columns = get_enabled_feature_columns(conn)
        except Exception:
            feature_columns = DEFAULT_FEATURE_COLUMNS

    if not feature_columns:
        feature_columns = DEFAULT_FEATURE_COLUMNS

    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT m.match_id, m.home_team_id, m.away_team_id, m.league_id,
                      m.season, m.match_date, m.result
               FROM matches m
               WHERE m.result IS NOT NULL
               ORDER BY m.match_date"""
        )
        columns = [desc[0] for desc in cursor.description]
        matches = [dict(zip(columns, row)) for row in cursor.fetchall()]

        if not matches:
            return pd.DataFrame(), pd.Series()

        features_list = []
        targets = []

        for match in matches:
            try:
                factors = calculate_all_factors(
                    conn, match["home_team_id"], match["away_team_id"],
                    match["league_id"], match["season"], match["match_date"]
                )

                # 只保留模型需要的特征
                feature_dict = {}
                for col in feature_columns:
                    feature_dict[col] = factors.get(col, 0.0)

                features_list.append(feature_dict)
                targets.append(RESULT_MAP.get(match["result"], 1))
            except Exception:
                continue

        if not features_list:
            return pd.DataFrame(), pd.Series()

        X = pd.DataFrame(features_list)
        y = pd.Series(targets)

        # 填充缺失值
        X = X.fillna(0.0)

        return X, y
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def train_logistic_regression(X: pd.DataFrame, y: pd.Series,
                               feature_columns: Optional[List[str]] = None) -> Tuple[LogisticRegression, StandardScaler, Dict[str, Any]]:
    """训练逻辑回归基线模型

    Args:
        X: 特征DataFrame
        y: 目标Series
        feature_columns: 使用的特征列名

    Returns:
        (模型, 标准化器, 评估指标)的元组
    """
    used_features = feature_columns or list(X.columns)

    # 标准化特征
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 时间序列分割
    tscv = TimeSeriesSplit(n_splits=CROSS_VALIDATION_FOLDS)

    # 训练模型
    model = LogisticRegression(
        max_iter=1000,
        random_state=RANDOM_SEED,
        solver="lbfgs",
        C=1.0,
    )

    # 交叉验证评估
    cv_scores = []
    for train_idx, val_idx in tscv.split(X_scaled):
        X_train, X_val = X_scaled[train_idx], X_scaled[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model.fit(X_train, y_train)
        y_pred = model.predict(X_val)
        y_prob = model.predict_proba(X_val)

        cv_scores.append({
            "accuracy": accuracy_score(y_val, y_pred),
            "f1_macro": f1_score(y_val, y_pred, average="macro"),
            "log_loss": log_loss(y_val, y_prob),
        })

    # 在全部数据上训练最终模型
    model.fit(X_scaled, y)

    metrics = {
        "model_type": "logistic_regression",
        "cv_accuracy": np.mean([s["accuracy"] for s in cv_scores]),
        "cv_f1_macro": np.mean([s["f1_macro"] for s in cv_scores]),
        "cv_log_loss": np.mean([s["log_loss"] for s in cv_scores]),
        "feature_count": len(used_features),
    }

    return model, scaler, metrics


def train_xgboost_model(X: pd.DataFrame, y: pd.Series,
                          feature_columns: Optional[List[str]] = None) -> Tuple[xgb.XGBClassifier, Optional[StandardScaler], Dict[str, Any]]:
    """训练XGBoost分类模型

    Args:
        X: 特征DataFrame
        y: 目标Series
        feature_columns: 使用的特征列名

    Returns:
        (模型, None, 评估指标)的元组
    """
    used_features = feature_columns or list(X.columns)

    tscv = TimeSeriesSplit(n_splits=CROSS_VALIDATION_FOLDS)

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=RANDOM_SEED,
        eval_metric="mlogloss",
        num_class=3,
        objective="multi:softprob",
    )

    # 交叉验证评估
    cv_scores = []
    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )
        y_pred = model.predict(X_val)
        y_prob = model.predict_proba(X_val)

        cv_scores.append({
            "accuracy": accuracy_score(y_val, y_pred),
            "f1_macro": f1_score(y_val, y_pred, average="macro"),
            "log_loss": log_loss(y_val, y_prob),
        })

    # 在全部数据上训练最终模型
    model.fit(X, y, verbose=False)

    # 获取特征重要性
    feature_importance = dict(zip(used_features, model.feature_importances_))
    top_features = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)[:10]

    metrics = {
        "model_type": "xgboost",
        "cv_accuracy": np.mean([s["accuracy"] for s in cv_scores]),
        "cv_f1_macro": np.mean([s["f1_macro"] for s in cv_scores]),
        "cv_log_loss": np.mean([s["log_loss"] for s in cv_scores]),
        "feature_count": len(used_features),
        "top_features": top_features,
    }

    return model, None, metrics


def save_model(model: Any, scaler: Optional[StandardScaler], model_name: str,
               feature_columns: Optional[List[str]] = None) -> str:
    """保存模型和标准化器到文件

    Args:
        model: 训练好的模型
        scaler: 标准化器（可能为None）
        model_name: 模型名称
        feature_columns: 使用的特征列名

    Returns:
        保存路径
    """
    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, f"{model_name}.joblib")
    save_data = {
        "model": model,
        "scaler": scaler,
        "feature_columns": feature_columns or DEFAULT_FEATURE_COLUMNS,
    }
    joblib.dump(save_data, model_path)
    return model_path


def load_model(model_name: str) -> Tuple[Any, Optional[StandardScaler], List[str]]:
    """加载模型和标准化器

    Args:
        model_name: 模型名称

    Returns:
        (模型, 标准化器, 特征列名)的元组
    """
    model_path = os.path.join(MODEL_DIR, f"{model_name}.joblib")
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"模型文件不存在: {model_path}")

    save_data = joblib.load(model_path)
    return save_data["model"], save_data.get("scaler"), save_data.get("feature_columns", DEFAULT_FEATURE_COLUMNS)


def train_and_save_all_models() -> Dict[str, Any]:
    """训练所有模型并保存，返回训练结果摘要

    Returns:
        训练结果摘要字典
    """
    conn = db.get_connection()

    # 获取启用的特征列
    try:
        feature_columns = get_enabled_feature_columns(conn)
    except Exception:
        feature_columns = DEFAULT_FEATURE_COLUMNS

    if not feature_columns:
        feature_columns = DEFAULT_FEATURE_COLUMNS

    try:
        X, y = prepare_training_data(conn, feature_columns)
    except Exception:
        conn.close()
        return {"error": "训练数据准备失败"}

    if X.empty or y.empty:
        return {"error": "训练数据为空，请先生成示例数据"}

    results = {}

    # 训练逻辑回归
    lr_model, lr_scaler, lr_metrics = train_logistic_regression(X, y, feature_columns)
    save_model(lr_model, lr_scaler, "win_loss_logistic", feature_columns)
    results["logistic_regression"] = lr_metrics

    # 训练XGBoost
    xgb_model, _, xgb_metrics = train_xgboost_model(X, y, feature_columns)
    save_model(xgb_model, None, "win_loss_xgboost", feature_columns)
    results["xgboost"] = xgb_metrics

    results["enabled_feature_count"] = len(feature_columns)
    results["enabled_features"] = feature_columns

    return results
