"""球队API路由，提供球队数据查询接口"""
from typing import Optional, List

from fastapi import APIRouter, Query

from app.models.schemas import TeamResponse, LeagueResponse
from app import database as db

router = APIRouter()


@router.get("/teams", response_model=List[TeamResponse], summary="获取球队列表")
async def api_get_teams(league_id: Optional[int] = Query(None, description="联赛ID筛选")):
    """获取所有球队，可按联赛ID筛选"""
    conn = db.get_connection()
    try:
        teams = db.get_all_teams(conn, league_id)
        return [TeamResponse(**team) for team in teams]
    finally:
        conn.close()


@router.get("/teams/{team_id}", response_model=TeamResponse, summary="获取球队详情")
async def api_get_team(team_id: int):
    """根据球队ID获取详情"""
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM teams WHERE team_id = ?", (team_id,))
        row = cursor.fetchone()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="球队不存在")
        return TeamResponse(**dict(row))
    finally:
        conn.close()


@router.get("/leagues", response_model=List[LeagueResponse], summary="获取联赛列表")
async def api_get_leagues():
    """获取所有联赛"""
    conn = db.get_connection()
    try:
        leagues = db.get_all_leagues(conn)
        return [LeagueResponse(**league) for league in leagues]
    finally:
        conn.close()
