"""预测服务模块，整合胜负预测和进球数预测，提供完整的预测功能"""
import os
from typing import Dict, Any, Optional, List

import numpy as np
import pandas as pd

from app.config import MODEL_DIR
from app.services.factor_calculator import calculate_all_factors, get_factor_display_info, FACTOR_NAMES
from app.ml.win_loss_model import (
    load_model as load_win_loss_model,
    RESULT_MAP_INV, FEATURE_COLUMNS as WIN_LOSS_FEATURES,
)
from app.ml.goals_model import (
    load_goals_model, predict_double_poisson,
    GOALS_FEATURE_COLUMNS, CONCEDED_FEATURE_COLUMNS,
)
from app import database as db


def predict_match(home_team_id: int, away_team_id: int, league_id: int,
                  season: str, match_date: Optional[str] = None,
                  model_type: str = "xgboost") -> Dict[str, Any]:
    """执行完整的比赛预测流程

    Args:
        home_team_id: 主队ID
        away_team_id: 客队ID
        league_id: 联赛ID
        season: 赛季
        match_date: 比赛日期
        model_type: 模型类型，xgboost或logistic

    Returns:
        完整的预测结果字典
    """
    conn = db.get_connection()
    try:
        # 1. 计算所有因子
        factors = calculate_all_factors(conn, home_team_id, away_team_id,
                                        league_id, season, match_date)

        # 2. 胜负预测
        win_loss_result = _predict_win_loss(factors, model_type)

        # 3. 进球数预测
        goals_result = _predict_goals(factors)

        # 4. 使用双泊松分布计算比分概率
        home_xg = goals_result["home_expected_goals"]
        away_xg = goals_result["away_expected_goals"]
        poisson_result = predict_double_poisson(home_xg, away_xg)

        # 5. 获取球队名称
        cursor = conn.cursor()
        cursor.execute("SELECT team_name FROM teams WHERE team_id = ?", (home_team_id,))
        home_row = cursor.fetchone()
        home_team_name = home_row["team_name"] if home_row else "未知"

        cursor.execute("SELECT team_name FROM teams WHERE team_id = ?", (away_team_id,))
        away_row = cursor.fetchone()
        away_team_name = away_row["team_name"] if away_row else "未知"

        # 6. 整合结果
        result = {
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_team_name": home_team_name,
            "away_team_name": away_team_name,
            "league_id": league_id,
            "season": season,
            "match_date": match_date,
            "model_type": model_type,
            # 胜负概率（优先使用双泊松结果，更精确）
            "home_win_prob": poisson_result["home_win_prob"],
            "draw_prob": poisson_result["draw_prob"],
            "away_win_prob": poisson_result["away_win_prob"],
            # 预期进球
            "home_expected_goals": round(home_xg, 2),
            "away_expected_goals": round(away_xg, 2),
            # 比分预测
            "most_likely_score": poisson_result["most_likely_score"],
            "over_25_prob": poisson_result["over_25_prob"],
            "both_teams_score_prob": poisson_result["both_teams_score_prob"],
            "score_probabilities": poisson_result["score_probabilities"],
            # 因子详情
            "factors": factors,
            "factor_details": _build_factor_details(factors),
        }

        return result
    finally:
        conn.close()


def _predict_win_loss(factors: Dict[str, float],
                      model_type: str = "xgboost") -> Dict[str, Any]:
    """使用胜负预测模型进行预测

    Args:
        factors: 因子字典
        model_type: 模型类型

    Returns:
        胜负预测结果
    """
    model_name = f"win_loss_{model_type}"

    try:
        model, scaler, feature_cols = load_win_loss_model(model_name)
    except FileNotFoundError:
        # 模型不存在时返回默认概率
        return {
            "home_win_prob": 0.45,
            "draw_prob": 0.27,
            "away_win_prob": 0.28,
            "model_loaded": False,
        }

    # 构建特征向量
    feature_values = []
    for col in feature_cols:
        feature_values.append(factors.get(col, 0.0))

    X = pd.DataFrame([feature_values], columns=feature_cols)
    X = X.fillna(0.0)

    # 标准化（如果需要）
    if scaler is not None:
        X = scaler.transform(X)

    # 预测
    probabilities = model.predict_proba(X)[0]

    return {
        "home_win_prob": round(float(probabilities[0]), 4),
        "draw_prob": round(float(probabilities[1]), 4),
        "away_win_prob": round(float(probabilities[2]), 4),
        "model_loaded": True,
    }


def _predict_goals(factors: Dict[str, float]) -> Dict[str, Any]:
    """使用进球数模型进行预测

    Args:
        factors: 因子字典

    Returns:
        进球数预测结果
    """
    try:
        home_model, away_model, home_features, away_features = load_goals_model()
    except FileNotFoundError:
        # 模型不存在时使用因子估算
        home_xg = factors.get("home_avg_expected_goals", 1.2) * factors.get("attack_defense_ratio", 1.0)
        away_xg = factors.get("away_avg_expected_goals", 0.9) * factors.get("defense_attack_ratio", 1.0)
        return {
            "home_expected_goals": round(max(0.3, min(home_xg, 4.0)), 2),
            "away_expected_goals": round(max(0.2, min(away_xg, 3.5)), 2),
            "model_loaded": False,
        }

    # 构建主队特征
    home_feature_values = [factors.get(col, 0.0) for col in home_features]
    away_feature_values = [factors.get(col, 0.0) for col in away_features]

    X = pd.DataFrame([home_feature_values + away_feature_values],
                     columns=[f"home_{col}" for col in home_features] +
                             [f"away_{col}" for col in away_features])
    X = X.fillna(0.0)

    home_xg = float(home_model.predict(X)[0])
    away_xg = float(away_model.predict(X)[0])

    # 限制合理范围
    home_xg = max(0.3, min(home_xg, 4.0))
    away_xg = max(0.2, min(away_xg, 3.5))

    return {
        "home_expected_goals": round(home_xg, 2),
        "away_expected_goals": round(away_xg, 2),
        "model_loaded": True,
    }


def _build_factor_details(factors: Dict[str, float]) -> List[Dict[str, Any]]:
    """构建因子详情列表，用于前端展示

    Args:
        factors: 因子字典

    Returns:
        因子详情列表
    """
    details = []
    for key, value in sorted(factors.items()):
        display_info = get_factor_display_info(key)
        details.append({
            "key": key,
            "name": display_info["name"],
            "value": value,
            "description": display_info["description"],
        })
    return details


def get_model_performance() -> Dict[str, Any]:
    """获取模型性能信息

    Returns:
        模型性能摘要
    """
    performance = {}

    # 检查模型文件是否存在
    for model_name in ["win_loss_logistic", "win_loss_xgboost", "goals_xgboost"]:
        model_path = os.path.join(MODEL_DIR, f"{model_name}.joblib")
        performance[model_name] = {
            "exists": os.path.exists(model_path),
            "path": model_path,
        }

    return performance


def get_historical_accuracy(conn=None, league_id: Optional[int] = None,
                           season: Optional[str] = None) -> Dict[str, Any]:
    """获取历史预测准确率统计

    Args:
        conn: 数据库连接
        league_id: 联赛ID筛选
        season: 赛季筛选

    Returns:
        准确率统计信息
    """
    if conn is None:
        conn = db.get_connection()

    try:
        cursor = conn.cursor()

        query = """
            SELECT p.*, m.home_goals, m.away_goals, m.result,
                   ht.team_name as home_team_name, at.team_name as away_team_name
            FROM predictions p
            JOIN matches m ON p.match_id = m.match_id
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            WHERE m.result IS NOT NULL
        """
        params = []

        if league_id:
            query += " AND m.league_id = ?"
            params.append(league_id)
        if season:
            query += " AND m.season = ?"
            params.append(season)

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        predictions = [dict(zip(columns, row)) for row in cursor.fetchall()]

        if not predictions:
            return {"total": 0, "accuracy": 0.0, "details": []}

        # 计算准确率
        correct = 0
        total = len(predictions)
        details = []

        for pred in predictions:
            # 根据概率判断预测结果
            probs = {
                "主胜": pred["home_win_prob"],
                "平局": pred["draw_prob"],
                "客胜": pred["away_win_prob"],
            }
            predicted = max(probs, key=probs.get)
            actual = pred["result"]
            is_correct = predicted == actual

            if is_correct:
                correct += 1

            details.append({
                "match": f"{pred['home_team_name']} vs {pred['away_team_name']}",
                "predicted": predicted,
                "actual": actual,
                "correct": is_correct,
                "home_win_prob": pred["home_win_prob"],
                "draw_prob": pred["draw_prob"],
                "away_win_prob": pred["away_win_prob"],
            })

        return {
            "total": total,
            "correct": correct,
            "accuracy": round(correct / total, 4) if total > 0 else 0.0,
            "details": details,
        }
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
