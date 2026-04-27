"""因子API路由，提供因子数据查询接口"""
from typing import Optional, List

from fastapi import APIRouter, Query

from app.models.schemas import FactorDetailResponse
from app.services.factor_calculator import (
    calculate_all_factors, get_factor_display_info, FACTOR_NAMES, FACTOR_DESCRIPTIONS,
)
from app import database as db

router = APIRouter()


@router.get("/factors/calculate", summary="计算比赛因子")
async def api_calculate_factors(
    home_team_id: int = Query(..., description="主队ID"),
    away_team_id: int = Query(..., description="客队ID"),
    league_id: int = Query(..., description="联赛ID"),
    season: str = Query(..., description="赛季"),
    match_date: Optional[str] = Query(None, description="比赛日期"),
):
    """计算指定比赛的所有因子值"""
    conn = db.get_connection()
    try:
        factors = calculate_all_factors(
            conn, home_team_id, away_team_id, league_id, season, match_date
        )

        # 附加因子展示信息
        result = []
        for key, value in sorted(factors.items()):
            display_info = get_factor_display_info(key)
            result.append({
                "key": key,
                "name": display_info["name"],
                "value": value,
                "description": display_info["description"],
            })

        return {"factors": result, "total_count": len(result)}
    finally:
        conn.close()


@router.get("/factors/list", summary="获取因子体系列表")
async def api_list_factors():
    """获取所有因子类型及其说明"""
    factors = []
    for key, name in FACTOR_NAMES.items():
        factors.append({
            "key": key,
            "name": name,
            "description": FACTOR_DESCRIPTIONS.get(key, ""),
        })
    return {"factors": factors, "total_count": len(factors)}


@router.get("/factors/{match_id}", summary="获取比赛因子")
async def api_get_match_factors(match_id: int):
    """获取已存储的比赛因子数据"""
    conn = db.get_connection()
    try:
        factors = db.get_factors_by_match(conn, match_id)
        return {"match_id": match_id, "factors": factors}
    finally:
        conn.close()
