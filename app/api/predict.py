"""预测API路由，提供比赛预测接口"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.models.schemas import PredictRequest, PredictionResponse, FactorDetailResponse
from app.services.prediction_service import predict_match, get_model_performance, get_historical_accuracy
from app import database as db

router = APIRouter()


@router.post("/predict", response_model=PredictionResponse, summary="预测比赛结果")
async def api_predict(request: PredictRequest):
    """根据主客队ID和联赛信息预测比赛结果

    Args:
        request: 预测请求体，包含主客队ID和联赛信息

    Returns:
        预测结果，包含胜负概率、预期进球、比分概率等
    """
    try:
        # 确定赛季
        season = _get_current_season(request.league_id)

        result = predict_match(
            home_team_id=request.home_team_id,
            away_team_id=request.away_team_id,
            league_id=request.league_id,
            season=season,
            match_date=request.match_date,
        )

        return PredictionResponse(
            match_id=0,
            home_team_name=result["home_team_name"],
            away_team_name=result["away_team_name"],
            home_win_prob=result["home_win_prob"],
            draw_prob=result["draw_prob"],
            away_win_prob=result["away_win_prob"],
            home_expected_goals=result["home_expected_goals"],
            away_expected_goals=result["away_expected_goals"],
            most_likely_score=result["most_likely_score"],
            over_25_prob=result.get("over_25_prob"),
            both_teams_score_prob=result.get("both_teams_score_prob"),
            score_probabilities=result.get("score_probabilities"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预测失败: {str(e)}")


@router.get("/predict/performance", summary="获取模型性能")
async def api_model_performance():
    """获取已训练模型的性能指标"""
    return get_model_performance()


@router.get("/predict/accuracy", summary="获取历史预测准确率")
async def api_prediction_accuracy(
    league_id: Optional[int] = Query(None, description="联赛ID"),
    season: Optional[str] = Query(None, description="赛季"),
):
    """获取历史预测的准确率统计"""
    return get_historical_accuracy(league_id=league_id, season=season)


@router.get("/predict/{home_team_id}/{away_team_id}", response_model=PredictionResponse, summary="快速预测")
async def api_quick_predict(
    home_team_id: int,
    away_team_id: int,
    league_id: int = Query(1, description="联赛ID"),
    match_date: Optional[str] = Query(None, description="比赛日期"),
):
    """通过URL参数快速预测比赛结果"""
    season = _get_current_season(league_id)

    try:
        result = predict_match(
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            league_id=league_id,
            season=season,
            match_date=match_date,
        )

        return PredictionResponse(
            match_id=0,
            home_team_name=result["home_team_name"],
            away_team_name=result["away_team_name"],
            home_win_prob=result["home_win_prob"],
            draw_prob=result["draw_prob"],
            away_win_prob=result["away_win_prob"],
            home_expected_goals=result["home_expected_goals"],
            away_expected_goals=result["away_expected_goals"],
            most_likely_score=result["most_likely_score"],
            over_25_prob=result.get("over_25_prob"),
            both_teams_score_prob=result.get("both_teams_score_prob"),
            score_probabilities=result.get("score_probabilities"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预测失败: {str(e)}")


def _get_current_season(league_id: int) -> str:
    """根据当前日期推算赛季字符串

    Args:
        league_id: 联赛ID

    Returns:
        赛季字符串，如"2025-2026"
    """
    from datetime import datetime
    now = datetime.now()
    if now.month >= 8:
        return f"{now.year}-{now.year + 1}"
    else:
        return f"{now.year - 1}-{now.year}"
