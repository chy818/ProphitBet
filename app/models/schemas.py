"""Pydantic数据模型，用于API请求/响应的数据验证和序列化"""
from typing import Optional, List
from pydantic import BaseModel, Field


class TeamResponse(BaseModel):
    """球队信息响应模型"""
    team_id: int = Field(..., description="球队ID")
    team_name: str = Field(..., description="球队名称")
    league_id: int = Field(..., description="联赛ID")
    home_ground: Optional[str] = Field(None, description="主场名称")
    founded_year: Optional[int] = Field(None, description="成立年份")


class LeagueResponse(BaseModel):
    """联赛信息响应模型"""
    league_id: int = Field(..., description="联赛ID")
    league_name: str = Field(..., description="联赛名称")
    country: Optional[str] = Field(None, description="国家")


class MatchResponse(BaseModel):
    """比赛信息响应模型"""
    match_id: int = Field(..., description="比赛ID")
    home_team_id: int = Field(..., description="主队ID")
    away_team_id: int = Field(..., description="客队ID")
    home_team_name: str = Field(..., description="主队名称")
    away_team_name: str = Field(..., description="客队名称")
    match_date: str = Field(..., description="比赛日期")
    league_id: int = Field(..., description="联赛ID")
    season: str = Field(..., description="赛季")
    home_goals: Optional[int] = Field(None, description="主队进球")
    away_goals: Optional[int] = Field(None, description="客队进球")
    result: Optional[str] = Field(None, description="比赛结果")


class MatchStatsResponse(BaseModel):
    """比赛统计响应模型"""
    stat_id: int = Field(..., description="统计ID")
    match_id: int = Field(..., description="比赛ID")
    team_id: int = Field(..., description="球队ID")
    expected_goals: Optional[float] = Field(None, description="预期进球")
    shots_on_target: Optional[int] = Field(None, description="射正次数")
    key_passes: Optional[int] = Field(None, description="关键传球")
    shots: Optional[int] = Field(None, description="射门次数")
    attacking_third_entries: Optional[int] = Field(None, description="进入进攻三区次数")
    set_pieces_shots: Optional[int] = Field(None, description="定位球创造射门次数")
    expected_goals_conceded: Optional[float] = Field(None, description="预期失球")
    shots_on_target_conceded: Optional[int] = Field(None, description="被射正次数")
    pressing_intensity: Optional[int] = Field(None, description="压迫强度")
    aerial_duel_success: Optional[float] = Field(None, description="空中对抗成功率")
    goalkeeper_save_rate: Optional[float] = Field(None, description="扑救成功率")


class FactorResponse(BaseModel):
    """因子信息响应模型"""
    factor_id: int = Field(..., description="因子ID")
    match_id: int = Field(..., description="比赛ID")
    team_id: int = Field(..., description="球队ID")
    factor_type: str = Field(..., description="因子类型")
    factor_value: float = Field(..., description="因子值")


class PredictionResponse(BaseModel):
    """预测结果响应模型"""
    prediction_id: Optional[int] = Field(None, description="预测ID")
    match_id: int = Field(..., description="比赛ID")
    home_team_name: str = Field(..., description="主队名称")
    away_team_name: str = Field(..., description="客队名称")
    home_win_prob: float = Field(..., description="主胜概率")
    draw_prob: float = Field(..., description="平局概率")
    away_win_prob: float = Field(..., description="客胜概率")
    home_expected_goals: float = Field(..., description="主队预期进球")
    away_expected_goals: float = Field(..., description="客队预期进球")
    most_likely_score: str = Field(..., description="最可能比分")
    over_25_prob: Optional[float] = Field(None, description="大于2.5球概率")
    both_teams_score_prob: Optional[float] = Field(None, description="两队都进球概率")
    score_probabilities: Optional[List[dict]] = Field(None, description="比分概率分布")


class PredictRequest(BaseModel):
    """预测请求模型"""
    home_team_id: int = Field(..., description="主队ID")
    away_team_id: int = Field(..., description="客队ID")
    league_id: int = Field(..., description="联赛ID")
    match_date: Optional[str] = Field(None, description="比赛日期，格式YYYY-MM-DD")


class FactorDetailResponse(BaseModel):
    """因子详情响应模型，包含因子名称和说明"""
    factor_type: str = Field(..., description="因子类型编码")
    factor_name: str = Field(..., description="因子中文名称")
    factor_value: float = Field(..., description="因子值")
    description: str = Field(..., description="因子说明")
