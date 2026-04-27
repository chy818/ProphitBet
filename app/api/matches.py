"""比赛API路由，提供比赛数据查询接口"""
from typing import Optional, List

from fastapi import APIRouter, Query

from app.models.schemas import MatchResponse
from app import database as db

router = APIRouter()


@router.get("/matches", response_model=List[MatchResponse], summary="获取比赛列表")
async def api_get_matches(
    league_id: Optional[int] = Query(None, description="联赛ID"),
    season: Optional[str] = Query(None, description="赛季"),
    team_id: Optional[int] = Query(None, description="球队ID"),
    limit: Optional[int] = Query(50, description="返回数量限制"),
):
    """获取比赛列表，支持多种筛选条件"""
    conn = db.get_connection()
    try:
        cursor = conn.cursor()

        query = """
            SELECT m.*, ht.team_name as home_team_name, at.team_name as away_team_name
            FROM matches m
            JOIN teams ht ON m.home_team_id = ht.team_id
            JOIN teams at ON m.away_team_id = at.team_id
            WHERE 1=1
        """
        params = []

        if league_id:
            query += " AND m.league_id = ?"
            params.append(league_id)
        if season:
            query += " AND m.season = ?"
            params.append(season)
        if team_id:
            query += " AND (m.home_team_id = ? OR m.away_team_id = ?)"
            params.extend([team_id, team_id])

        query += " ORDER BY m.match_date DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        matches = [dict(zip(columns, row)) for row in cursor.fetchall()]

        return [MatchResponse(**match) for match in matches]
    finally:
        conn.close()


@router.get("/matches/head-to-head", response_model=List[MatchResponse], summary="获取交锋记录")
async def api_get_head_to_head(
    team_a_id: int = Query(..., description="球队A的ID"),
    team_b_id: int = Query(..., description="球队B的ID"),
    limit: int = Query(6, description="返回数量"),
):
    """获取两支球队的交锋历史记录"""
    conn = db.get_connection()
    try:
        h2h = db.get_head_to_head_matches(conn, team_a_id, team_b_id, limit)
        return [MatchResponse(**match) for match in h2h]
    finally:
        conn.close()


@router.get("/matches/{match_id}", summary="获取比赛详情")
async def api_get_match_detail(match_id: int):
    """获取单场比赛的详细信息，包括统计数据"""
    conn = db.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT m.*, ht.team_name as home_team_name, at.team_name as away_team_name
               FROM matches m
               JOIN teams ht ON m.home_team_id = ht.team_id
               JOIN teams at ON m.away_team_id = at.team_id
               WHERE m.match_id = ?""",
            (match_id,)
        )
        row = cursor.fetchone()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="比赛不存在")

        match_data = dict(row)

        # 获取统计数据
        stats = db.get_match_stats(conn, match_id)
        match_data["stats"] = stats

        return match_data
    finally:
        conn.close()
