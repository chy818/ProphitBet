"""数据采集模块，负责从外部数据源获取比赛数据并生成示例数据"""
import random
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd
import requests

from app.config import SUPPORTED_LEAGUES, RANDOM_SEED, JUHE_API_KEY, JUHE_API_BASE_URL, JUHE_LEAGUE_MAP
from app import database as db


# 英超球队数据
PREMIER_LEAGUE_TEAMS = [
    {"name": "曼城", "home_ground": "伊蒂哈德球场", "founded_year": 1880},
    {"name": "阿森纳", "home_ground": "酋长球场", "founded_year": 1886},
    {"name": "利物浦", "home_ground": "安菲尔德球场", "founded_year": 1892},
    {"name": "切尔西", "home_ground": "斯坦福桥球场", "founded_year": 1905},
    {"name": "曼联", "home_ground": "老特拉福德球场", "founded_year": 1878},
    {"name": "热刺", "home_ground": "托特纳姆热刺球场", "founded_year": 1882},
    {"name": "纽卡斯尔", "home_ground": "圣詹姆斯公园", "founded_year": 1892},
    {"name": "阿斯顿维拉", "home_ground": "维拉公园球场", "founded_year": 1874},
    {"name": "布莱顿", "home_ground": "美国运通社区球场", "founded_year": 1901},
    {"name": "西汉姆", "home_ground": "伦敦体育场", "founded_year": 1895},
    {"name": "水晶宫", "home_ground": "塞尔赫斯特公园", "founded_year": 1905},
    {"name": "布伦特福德", "home_ground": "社区球场", "founded_year": 1889},
    {"name": "富勒姆", "home_ground": "克拉文农场", "founded_year": 1879},
    {"name": "狼队", "home_ground": "莫利纽克斯球场", "founded_year": 1877},
    {"name": "伯恩茅斯", "home_ground": "活力球场", "founded_year": 1899},
    {"name": "埃弗顿", "home_ground": "古迪逊公园", "founded_year": 1878},
    {"name": "诺丁汉森林", "home_ground": "城市足球场", "founded_year": 1865},
    {"name": "莱斯特城", "home_ground": "皇权球场", "founded_year": 1884},
    {"name": "伊普斯维奇", "home_ground": "波特曼路球场", "founded_year": 1878},
    {"name": "南安普顿", "home_ground": "圣玛丽球场", "founded_year": 1885},
]

# 西甲球队数据
LA_LIGA_TEAMS = [
    {"name": "皇家马德里", "home_ground": "伯纳乌球场", "founded_year": 1902},
    {"name": "巴塞罗那", "home_ground": "诺坎普球场", "founded_year": 1899},
    {"name": "马德里竞技", "home_ground": "大都会球场", "founded_year": 1903},
    {"name": "皇家社会", "home_ground": "阿诺埃塔球场", "founded_year": 1909},
    {"name": "比利亚雷亚尔", "home_ground": "陶瓷球场", "founded_year": 1923},
    {"name": "贝蒂斯", "home_ground": "贝尼托比利亚马林", "founded_year": 1907},
    {"name": "赫罗纳", "home_ground": "蒙蒂利维球场", "founded_year": 1930},
    {"name": "毕尔巴鄂竞技", "home_ground": "圣马梅斯球场", "founded_year": 1898},
    {"name": "塞维利亚", "home_ground": "皮斯胡安球场", "founded_year": 1890},
    {"name": "瓦伦西亚", "home_ground": "梅斯塔利亚球场", "founded_year": 1919},
]

# 德甲球队数据
BUNDESLIGA_TEAMS = [
    {"name": "拜仁慕尼黑", "home_ground": "安联球场", "founded_year": 1900},
    {"name": "勒沃库森", "home_ground": "拜耳竞技场", "founded_year": 1904},
    {"name": "多特蒙德", "home_ground": "西格纳伊度纳公园", "founded_year": 1909},
    {"name": "莱比锡", "home_ground": "红牛竞技场", "founded_year": 2009},
    {"name": "斯图加特", "home_ground": "梅赛德斯奔驰竞技场", "founded_year": 1893},
    {"name": "法兰克福", "home_ground": "德意志银行公园", "founded_year": 1899},
    {"name": "沃尔夫斯堡", "home_ground": "大众汽车竞技场", "founded_year": 1945},
    {"name": "弗赖堡", "home_ground": "欧洲公园球场", "founded_year": 1904},
    {"name": "霍芬海姆", "home_ground": "莱茵内卡竞技场", "founded_year": 1899},
    {"name": "门兴格拉德巴赫", "home_ground": "普鲁士公园", "founded_year": 1900},
]

# 意甲球队数据
SERIE_A_TEAMS = [
    {"name": "国际米兰", "home_ground": "梅阿查球场", "founded_year": 1908},
    {"name": "AC米兰", "home_ground": "圣西罗球场", "founded_year": 1899},
    {"name": "尤文图斯", "home_ground": "安联球场", "founded_year": 1897},
    {"name": "那不勒斯", "home_ground": "迭戈马拉多纳球场", "founded_year": 1926},
    {"name": "亚特兰大", "home_ground": "蓝色意大利球场", "founded_year": 1907},
    {"name": "罗马", "home_ground": "奥林匹克球场", "founded_year": 1927},
    {"name": "拉齐奥", "home_ground": "奥林匹克球场", "founded_year": 1900},
    {"name": "佛罗伦萨", "home_ground": "弗兰基球场", "founded_year": 1926},
    {"name": "博洛尼亚", "home_ground": "达拉拉球场", "founded_year": 1909},
    {"name": "都灵", "home_ground": "奥林匹克大球场", "founded_year": 1906},
]

# 法甲球队数据
LIGUE_1_TEAMS = [
    {"name": "巴黎圣日耳曼", "home_ground": "王子公园球场", "founded_year": 1970},
    {"name": "马赛", "home_ground": "韦洛德罗姆球场", "founded_year": 1899},
    {"name": "摩纳哥", "home_ground": "路易二世球场", "founded_year": 1924},
    {"name": "里尔", "home_ground": "皮埃尔莫鲁瓦球场", "founded_year": 1944},
    {"name": "里昂", "home_ground": "灯光球场", "founded_year": 1950},
    {"name": "尼斯", "home_ground": "安联里维耶拉球场", "founded_year": 1904},
    {"name": "朗斯", "home_ground": "博莱尔球场", "founded_year": 1906},
    {"name": "雷恩", "home_ground": "洛里昂路球场", "founded_year": 1901},
    {"name": "斯特拉斯堡", "home_ground": "梅纳乌球场", "founded_year": 1906},
    {"name": "图卢兹", "home_ground": "市政球场", "founded_year": 1970},
]

# 各联赛球队数据映射
LEAGUE_TEAMS_MAP = {
    1: PREMIER_LEAGUE_TEAMS,
    2: LA_LIGA_TEAMS,
    3: BUNDESLIGA_TEAMS,
    4: SERIE_A_TEAMS,
    5: LIGUE_1_TEAMS,
}

# 球队实力等级（用于生成更真实的比赛数据）
TEAM_STRENGTH = {
    "曼城": 0.92, "阿森纳": 0.88, "利物浦": 0.90, "切尔西": 0.78,
    "曼联": 0.75, "热刺": 0.76, "纽卡斯尔": 0.74, "阿斯顿维拉": 0.73,
    "布莱顿": 0.70, "西汉姆": 0.68, "水晶宫": 0.62, "布伦特福德": 0.65,
    "富勒姆": 0.63, "狼队": 0.60, "伯恩茅斯": 0.61, "埃弗顿": 0.58,
    "诺丁汉森林": 0.59, "莱斯特城": 0.57, "伊普斯维奇": 0.45, "南安普顿": 0.43,
    "皇家马德里": 0.93, "巴塞罗那": 0.89, "马德里竞技": 0.82,
    "皇家社会": 0.72, "比利亚雷亚尔": 0.71, "贝蒂斯": 0.68,
    "赫罗纳": 0.70, "毕尔巴鄂竞技": 0.69, "塞维利亚": 0.67, "瓦伦西亚": 0.60,
    "拜仁慕尼黑": 0.91, "勒沃库森": 0.85, "多特蒙德": 0.82,
    "莱比锡": 0.78, "斯图加特": 0.72, "法兰克福": 0.70,
    "沃尔夫斯堡": 0.65, "弗赖堡": 0.66, "霍芬海姆": 0.64, "门兴格拉德巴赫": 0.60,
    "国际米兰": 0.88, "AC米兰": 0.80, "尤文图斯": 0.82,
    "那不勒斯": 0.81, "亚特兰大": 0.79, "罗马": 0.73,
    "拉齐奥": 0.72, "佛罗伦萨": 0.70, "博洛尼亚": 0.68, "都灵": 0.62,
    "巴黎圣日耳曼": 0.90, "马赛": 0.72, "摩纳哥": 0.74,
    "里尔": 0.70, "里昂": 0.68, "尼斯": 0.66,
    "朗斯": 0.69, "雷恩": 0.64, "斯特拉斯堡": 0.58, "图卢兹": 0.57,
}


def generate_sample_data(seasons: int = 3) -> None:
    """生成模拟历史数据，包括联赛、球队、比赛和统计数据

    模拟数据用于模型训练，覆盖过去N个赛季。
    当前赛季的模拟数据后续会被API真实数据覆盖。

    Args:
        seasons: 生成数据的赛季数量
    """
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    with db.get_db() as conn:
        # 插入联赛数据
        for league_id, league_name in SUPPORTED_LEAGUES.items():
            db.insert_league(conn, league_id, league_name)

        # 插入球队数据并记录team_id映射
        team_id_map = {}
        for league_id, teams in LEAGUE_TEAMS_MAP.items():
            for team_info in teams:
                # 检查球队是否已存在
                existing_id = db.get_team_id_by_name(conn, team_info["name"])
                if existing_id:
                    team_id_map[team_info["name"]] = existing_id
                else:
                    team_id = db.insert_team(
                        conn, team_info["name"], league_id,
                        team_info["home_ground"], team_info["founded_year"]
                    )
                    team_id_map[team_info["name"]] = team_id

        # 为每个赛季生成比赛数据（不含当前赛季，当前赛季由API提供）
        current_year = datetime.now().year
        # 当前赛季：如果当前月份>=8，当前赛季为 current_year-(current_year+1)
        # 否则当前赛季为 (current_year-1)-current_year
        if datetime.now().month >= 8:
            current_season_start = current_year
        else:
            current_season_start = current_year - 1

        # 只为当前赛季之前的赛季生成模拟数据
        for season_offset in range(seasons):
            season_year = current_season_start - seasons + season_offset
            season_str = f"{season_year}-{season_year + 1}"
            season_start = datetime(season_year, 8, 1)
            season_end = datetime(season_year + 1, 5, 31)

            for league_id, teams in LEAGUE_TEAMS_MAP.items():
                team_names = [t["name"] for t in teams]
                team_count = len(team_names)

                # 生成双循环赛程（主客场各一次）
                for round_num in range(2):
                    for i in range(team_count):
                        for j in range(i + 1, team_count):
                            if round_num == 0:
                                home_name = team_names[i]
                                away_name = team_names[j]
                            else:
                                home_name = team_names[j]
                                away_name = team_names[i]

                            # 随机生成比赛日期
                            days_offset = random.randint(0, (season_end - season_start).days)
                            match_date = (season_start + timedelta(days=days_offset)).strftime("%Y-%m-%d")

                            # 根据球队实力生成比赛结果
                            home_strength = TEAM_STRENGTH.get(home_name, 0.6)
                            away_strength = TEAM_STRENGTH.get(away_name, 0.6)

                            # 主场优势加成
                            home_advantage = 0.06
                            effective_home = home_strength + home_advantage

                            # 生成进球数（使用泊松分布）
                            home_expected = effective_home * 3.0
                            away_expected = away_strength * 2.5
                            home_goals = np.random.poisson(home_expected * 0.45)
                            away_goals = np.random.poisson(away_expected * 0.40)

                            # 确定比赛结果
                            if home_goals > away_goals:
                                result = "主胜"
                            elif home_goals == away_goals:
                                result = "平局"
                            else:
                                result = "客胜"

                            home_team_id = team_id_map[home_name]
                            away_team_id = team_id_map[away_name]

                            match_id = db.insert_match(
                                conn, home_team_id, away_team_id,
                                match_date, league_id, season_str,
                                home_goals, away_goals, result
                            )

                            # 生成比赛统计数据
                            _generate_match_stats(conn, match_id, home_team_id, away_team_id,
                                                  home_name, away_name, home_goals, away_goals)


def _generate_match_stats(conn: sqlite3.Connection, match_id: int,
                          home_team_id: int, away_team_id: int,
                          home_name: str, away_name: str,
                          home_goals: int, away_goals: int) -> None:
    """为单场比赛生成统计数据

    Args:
        conn: 数据库连接
        match_id: 比赛ID
        home_team_id: 主队ID
        away_team_id: 客队ID
        home_name: 主队名称
        away_name: 客队名称
        home_goals: 主队进球数
        away_goals: 客队进球数
    """
    home_strength = TEAM_STRENGTH.get(home_name, 0.6)
    away_strength = TEAM_STRENGTH.get(away_name, 0.6)

    # 主队统计数据
    home_stats = {
        "expected_goals": round(max(0.3, home_strength * 2.8 + random.gauss(0, 0.3)), 2),
        "shots_on_target": max(1, int(home_strength * 8 + random.gauss(0, 2))),
        "key_passes": max(2, int(home_strength * 12 + random.gauss(0, 3))),
        "shots": max(3, int(home_strength * 14 + random.gauss(0, 3))),
        "attacking_third_entries": max(10, int(home_strength * 50 + random.gauss(0, 8))),
        "set_pieces_shots": max(0, int(home_strength * 4 + random.gauss(0, 1.5))),
        "expected_goals_conceded": round(max(0.2, (1 - home_strength) * 2.5 + random.gauss(0, 0.2)), 2),
        "shots_on_target_conceded": max(1, int((1 - home_strength) * 7 + random.gauss(0, 2))),
        "pressing_intensity": max(30, int(home_strength * 180 + random.gauss(0, 20))),
        "aerial_duel_success": round(max(0.2, min(0.8, home_strength * 0.55 + random.gauss(0, 0.05))), 2),
        "goalkeeper_save_rate": round(max(0.4, min(0.95, 0.65 + random.gauss(0, 0.08))), 2),
    }

    # 客队统计数据
    away_stats = {
        "expected_goals": round(max(0.2, away_strength * 2.3 + random.gauss(0, 0.3)), 2),
        "shots_on_target": max(1, int(away_strength * 6 + random.gauss(0, 2))),
        "key_passes": max(1, int(away_strength * 10 + random.gauss(0, 3))),
        "shots": max(2, int(away_strength * 12 + random.gauss(0, 3))),
        "attacking_third_entries": max(8, int(away_strength * 40 + random.gauss(0, 8))),
        "set_pieces_shots": max(0, int(away_strength * 3 + random.gauss(0, 1.5))),
        "expected_goals_conceded": round(max(0.3, (1 - away_strength) * 3.0 + random.gauss(0, 0.2)), 2),
        "shots_on_target_conceded": max(1, int((1 - away_strength) * 8 + random.gauss(0, 2))),
        "pressing_intensity": max(25, int(away_strength * 160 + random.gauss(0, 20))),
        "aerial_duel_success": round(max(0.2, min(0.8, away_strength * 0.52 + random.gauss(0, 0.05))), 2),
        "goalkeeper_save_rate": round(max(0.4, min(0.95, 0.62 + random.gauss(0, 0.08))), 2),
    }

    db.insert_match_stats(conn, match_id, home_team_id, home_stats)
    db.insert_match_stats(conn, match_id, away_team_id, away_stats)


def fetch_external_data(api_key: Optional[str] = None) -> None:
    """从聚合数据API获取实时数据，补充当前赛季的真实比赛结果

    API只提供近期赛程，不提供完整历史数据。
    因此需要先通过 generate_sample_data 生成历史模拟数据。

    Args:
        api_key: API密钥，应从环境变量获取，不应硬编码
    """
    if not api_key:
        api_key = JUHE_API_KEY

    if not api_key:
        print("警告: 未配置API密钥，跳过实时数据获取")
        return

    with db.get_db() as conn:
        # 获取所有支持的联赛数据
        for league_name, juhe_league in JUHE_LEAGUE_MAP.items():
            print(f"正在获取 {league_name} 实时数据...")

            # 1. 获取联赛积分榜数据（用于更新球队列表）
            rank_data = _fetch_league_rank(juhe_league, api_key)
            if rank_data:
                _process_rank_data(conn, league_name, rank_data)

            # 2. 获取近期赛程数据（用于补充真实比赛结果）
            schedule_data = _fetch_league_schedule(juhe_league, api_key)
            if schedule_data:
                _process_schedule_data(conn, league_name, schedule_data)


def _fetch_league_rank(league_type: str, api_key: str) -> Optional[Dict[str, Any]]:
    """获取联赛积分榜数据

    Args:
        league_type: 联赛类型（聚合数据API格式）
        api_key: API密钥

    Returns:
        积分榜数据字典，失败返回None
    """
    try:
        url = f"{JUHE_API_BASE_URL}/rank"
        params = {
            "key": api_key,
            "type": league_type
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("error_code") == 0:
            return data.get("result")
        else:
            print(f"  获取积分榜失败: {data.get('reason')}")
            return None
    except Exception as e:
        print(f"  获取积分榜异常: {str(e)}")
        return None


def _fetch_league_schedule(league_type: str, api_key: str) -> Optional[Dict[str, Any]]:
    """获取近期赛程数据

    Args:
        league_type: 联赛类型（聚合数据API格式）
        api_key: API密钥

    Returns:
        赛程数据字典，失败返回None
    """
    try:
        url = f"{JUHE_API_BASE_URL}/query"
        params = {
            "key": api_key,
            "type": league_type
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("error_code") == 0:
            return data.get("result")
        else:
            print(f"  获取赛程失败: {data.get('reason')}")
            return None
    except Exception as e:
        print(f"  获取赛程异常: {str(e)}")
        return None


def _process_rank_data(conn: sqlite3.Connection, league_name: str, rank_data: Dict[str, Any]) -> None:
    """处理积分榜数据，更新球队列表

    Args:
        conn: 数据库连接
        league_name: 联赛名称
        rank_data: 积分榜数据
    """
    league_id = _get_or_create_league(conn, league_name)

    ranking = rank_data.get("ranking", [])
    for rank_info in ranking:
        team_name = rank_info.get("team")
        if not team_name:
            continue

        # 获取或创建球队
        _get_or_create_team(conn, team_name, league_id)


def _process_schedule_data(conn: sqlite3.Connection, league_name: str, schedule_data: Dict[str, Any]) -> None:
    """处理赛程数据，将已完赛的真实比赛结果写入数据库

    对于已存在的同日期同对阵比赛，用真实结果替换模拟数据。

    Args:
        conn: 数据库连接
        league_name: 联赛名称
        schedule_data: 赛程数据
    """
    league_id = _get_or_create_league(conn, league_name)

    matchs = schedule_data.get("matchs", [])
    for match_day in matchs:
        date = match_day.get("date")
        matches = match_day.get("list", [])

        for match in matches:
            status = match.get("status")
            # 只处理已完赛的比赛
            if status != "3":
                continue

            home_team = match.get("team1")
            away_team = match.get("team2")
            home_score = match.get("team1_score")
            away_score = match.get("team2_score")

            if not all([home_team, away_team, home_score, away_score]):
                continue

            try:
                home_score = int(home_score)
                away_score = int(away_score)
            except (ValueError, TypeError):
                continue

            # 获取或创建球队
            home_team_id = _get_or_create_team(conn, home_team, league_id)
            away_team_id = _get_or_create_team(conn, away_team, league_id)

            # 确定比赛结果
            if home_score > away_score:
                result = "主胜"
            elif home_score == away_score:
                result = "平局"
            else:
                result = "客胜"

            # 计算正确的赛季
            match_date = datetime.strptime(date, "%Y-%m-%d")
            if match_date.month >= 8:
                season = f"{match_date.year}-{match_date.year + 1}"
            else:
                season = f"{match_date.year - 1}-{match_date.year}"

            # 检查比赛是否已存在
            if not _match_exists(conn, home_team_id, away_team_id, date):
                # 插入新的比赛数据
                match_id = db.insert_match(
                    conn, home_team_id, away_team_id, date, league_id,
                    season, home_score, away_score, result
                )

                # 生成基本的比赛统计数据
                _generate_match_stats(conn, match_id, home_team_id, away_team_id,
                                     home_team, away_team, home_score, away_score)


def _get_or_create_league(conn: sqlite3.Connection, league_name: str) -> int:
    """获取或创建联赛

    Args:
        conn: 数据库连接
        league_name: 联赛名称

    Returns:
        联赛ID
    """
    league_id = db.get_league_id_by_name(conn, league_name)
    if league_id:
        return league_id

    # 查找支持的联赛ID
    league_id = None
    for lid, name in SUPPORTED_LEAGUES.items():
        if name == league_name:
            league_id = lid
            break

    if not league_id:
        # 对于不支持的联赛，使用999
        league_id = 999

    db.insert_league(conn, league_id, league_name)
    return league_id


# API返回的球队名称与模拟数据的名称映射
# 用于将API返回的不同名称统一到模拟数据的名称
TEAM_NAME_MAP = {
    "纽卡斯尔联": "纽卡斯尔",
    "托特纳姆热刺": "热刺",
    "西汉姆联": "西汉姆",
    "莱斯特城": "莱斯特城",
    "曼彻斯特联": "曼联",
    "曼彻斯特城": "曼城",
    "诺丁汉森林": "诺丁汉森林",
    "阿斯顿维拉": "阿斯顿维拉",
    "布伦特福德": "布伦特福德",
    "伯恩茅斯": "伯恩茅斯",
    "水晶宫": "水晶宫",
    "布赖顿": "布莱顿",
    "利兹联": "利兹联",
    "富勒姆": "富勒姆",
    "伊普斯维奇": "伊普斯维奇",
    "埃弗顿": "埃弗顿",
    "南安普顿": "南安普顿",
    "狼队": "狼队",
    "切尔西": "切尔西",
    "阿森纳": "阿森纳",
    "利物浦": "利物浦",
    "伯恩利": "伯恩利",
    "皇家马德里": "皇家马德里",
    "巴塞罗那": "巴塞罗那",
    "马德里竞技": "马德里竞技",
    "皇家社会": "皇家社会",
    "比利亚雷亚尔": "比利亚雷亚尔",
    "贝蒂斯": "贝蒂斯",
    "赫罗纳": "赫罗纳",
    "毕尔巴鄂竞技": "毕尔巴鄂竞技",
    "塞维利亚": "塞维利亚",
    "瓦伦西亚": "瓦伦西亚",
    "拜仁慕尼黑": "拜仁慕尼黑",
    "勒沃库森": "勒沃库森",
    "多特蒙德": "多特蒙德",
    "RB莱比锡": "莱比锡",
    "斯图加特": "斯图加特",
    "法兰克福": "法兰克福",
    "沃尔夫斯堡": "沃尔夫斯堡",
    "弗赖堡": "弗赖堡",
    "霍芬海姆": "霍芬海姆",
    "门兴格拉德巴赫": "门兴格拉德巴赫",
    "国际米兰": "国际米兰",
    "AC米兰": "AC米兰",
    "尤文图斯": "尤文图斯",
    "那不勒斯": "那不勒斯",
    "亚特兰大": "亚特兰大",
    "罗马": "罗马",
    "拉齐奥": "拉齐奥",
    "佛罗伦萨": "佛罗伦萨",
    "博洛尼亚": "博洛尼亚",
    "都灵": "都灵",
    "巴黎圣日耳曼": "巴黎圣日耳曼",
    "马赛": "马赛",
    "摩纳哥": "摩纳哥",
    "里尔": "里尔",
    "里昂": "里昂",
    "尼斯": "尼斯",
    "朗斯": "朗斯",
    "雷恩": "雷恩",
    "斯特拉斯堡": "斯特拉斯堡",
    "图卢兹": "图卢兹",
}


def _normalize_team_name(team_name: str) -> str:
    """将API返回的球队名称标准化为模拟数据的名称

    Args:
        team_name: API返回的球队名称

    Returns:
        标准化后的球队名称
    """
    return TEAM_NAME_MAP.get(team_name, team_name)


def _get_or_create_team(conn: sqlite3.Connection, team_name: str, league_id: int) -> int:
    """获取或创建球队

    优先使用标准化后的名称查找已有球队，避免重复创建。

    Args:
        conn: 数据库连接
        team_name: 球队名称
        league_id: 联赛ID

    Returns:
        球队ID
    """
    # 标准化球队名称
    normalized_name = _normalize_team_name(team_name)

    # 先用标准化名称查找
    team_id = db.get_team_id_by_name(conn, normalized_name)
    if team_id:
        return team_id

    # 再用原始名称查找
    team_id = db.get_team_id_by_name(conn, team_name)
    if team_id:
        return team_id

    # 创建新球队
    team_id = db.insert_team(
        conn, team_name, league_id, f"{team_name}主场", datetime.now().year - 10
    )
    return team_id


def _match_exists(conn: sqlite3.Connection, home_team_id: int, away_team_id: int, match_date: str) -> bool:
    """检查比赛是否已存在

    Args:
        conn: 数据库连接
        home_team_id: 主队ID
        away_team_id: 客队ID
        match_date: 比赛日期

    Returns:
        是否已存在
    """
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT match_id FROM matches
            WHERE home_team_id = ? AND away_team_id = ? AND match_date = ?
            """,
            (home_team_id, away_team_id, match_date)
        )
        return cursor.fetchone() is not None
    except Exception:
        return False
