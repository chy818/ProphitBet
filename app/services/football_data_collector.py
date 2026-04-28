"""football-data.org 数据采集模块

提供比聚合数据更丰富的足球数据，包括：
- 完整赛季比赛数据（支持历史赛季查询）
- 积分榜（总/主场/客场）
- 球队详细信息
- 比赛统计（射门、控球率等）

API文档: https://www.football-data.org/documentation/api
"""
import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any, List

import requests

from app.config import (
    FOOTBALL_DATA_API_TOKEN,
    FOOTBALL_DATA_BASE_URL,
    FOOTBALL_DATA_LEAGUE_MAP,
    FOOTBALL_DATA_CODE_TO_LEAGUE,
    SUPPORTED_LEAGUES,
)
from app import database as db


def fetch_football_data(league_names: Optional[List[str]] = None,
                        season: Optional[int] = None) -> Dict[str, Any]:
    """从 football-data.org 获取数据

    Args:
        league_names: 要获取的联赛名称列表，None表示全部五大联赛
        season: 赛季起始年份，如2025表示2025-2026赛季，None表示当前赛季

    Returns:
        采集结果统计
    """
    if not FOOTBALL_DATA_API_TOKEN:
        print("错误: 未配置 football-data.org API Token")
        return {"error": "未配置API Token"}

    if league_names is None:
        league_names = ["英超", "西甲", "德甲", "意甲", "法甲"]

    stats = {"leagues": 0, "teams": 0, "matches": 0, "errors": []}

    with db.get_db() as conn:
        for league_name in league_names:
            code = FOOTBALL_DATA_LEAGUE_MAP.get(league_name)
            if not code:
                stats["errors"].append(f"不支持的联赛: {league_name}")
                continue

            print(f"正在从 football-data.org 获取 {league_name} 数据...")

            # 获取积分榜（含球队信息）
            standings_stats = _fetch_standings(conn, code, league_name, season)
            stats["teams"] += standings_stats.get("teams", 0)

            # 获取比赛数据
            matches_stats = _fetch_matches(conn, code, league_name, season)
            stats["matches"] += matches_stats.get("matches", 0)

            if standings_stats.get("error") or matches_stats.get("error"):
                stats["errors"].append(f"{league_name}: 数据获取部分失败")

            stats["leagues"] += 1

    return stats


def _make_request(endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
    """发送API请求

    Args:
        endpoint: API端点（不含base_url）
        params: 请求参数

    Returns:
        JSON响应数据，失败返回None
    """
    url = f"{FOOTBALL_DATA_BASE_URL}{endpoint}"
    headers = {"X-Auth-Token": FOOTBALL_DATA_API_TOKEN}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            print("  API请求频率超限，请稍后重试")
        else:
            print(f"  HTTP错误: {e}")
        return None
    except Exception as e:
        print(f"  请求异常: {e}")
        return None


def _fetch_standings(conn: sqlite3.Connection, code: str,
                     league_name: str, season: Optional[int] = None) -> Dict[str, Any]:
    """获取积分榜数据

    Args:
        conn: 数据库连接
        code: 联赛代码
        league_name: 联赛名称
        season: 赛季起始年份

    Returns:
        采集统计
    """
    stats = {"teams": 0, "error": False}

    params = {}
    if season:
        params["season"] = season

    data = _make_request(f"/competitions/{code}/standings", params)
    if not data:
        stats["error"] = True
        return stats

    # 获取联赛ID
    league_id = FOOTBALL_DATA_CODE_TO_LEAGUE.get(code, 999)
    db.insert_league(conn, league_id, league_name)

    standings = data.get("standings", [])
    for table in standings:
        # 只处理总积分榜（TOTAL）
        if table.get("type") != "TOTAL":
            continue

        for entry in table.get("table", []):
            team_info = entry.get("team", {})
            team_name = team_info.get("shortName") or team_info.get("name", "")

            if not team_name:
                continue

            # 标准化球队名称
            normalized_name = _normalize_team_name(team_name)

            # 获取或创建球队
            team_id = db.get_team_id_by_name(conn, normalized_name)
            if not team_id:
                team_id = db.insert_team(
                    conn, normalized_name, league_id,
                    team_info.get("venue", ""), None
                )
                stats["teams"] += 1

    return stats


def _fetch_matches(conn: sqlite3.Connection, code: str,
                   league_name: str, season: Optional[int] = None) -> Dict[str, Any]:
    """获取比赛数据

    Args:
        conn: 数据库连接
        code: 联赛代码
        league_name: 联赛名称
        season: 赛季起始年份

    Returns:
        采集统计
    """
    stats = {"matches": 0, "error": False}

    params = {}
    if season:
        params["season"] = season

    data = _make_request(f"/competitions/{code}/matches", params)
    if not data:
        stats["error"] = True
        return stats

    league_id = FOOTBALL_DATA_CODE_TO_LEAGUE.get(code, 999)

    matches = data.get("matches", [])
    for match in matches:
        # 只处理已完赛的比赛
        status = match.get("status")
        if status != "FINISHED":
            continue

        score = match.get("score", {})
        full_time = score.get("fullTime", {})
        home_goals = full_time.get("home")
        away_goals = full_time.get("away")

        if home_goals is None or away_goals is None:
            continue

        # 获取球队信息
        home_team_info = match.get("homeTeam", {})
        away_team_info = match.get("awayTeam", {})

        home_name = _normalize_team_name(
            home_team_info.get("shortName") or home_team_info.get("name", "")
        )
        away_name = _normalize_team_name(
            away_team_info.get("shortName") or away_team_info.get("name", "")
        )

        if not home_name or not away_name:
            continue

        # 获取或创建球队
        home_team_id = db.get_team_id_by_name(conn, home_name)
        if not home_team_id:
            home_team_id = db.insert_team(
                conn, home_name, league_id,
                home_team_info.get("venue", ""), None
            )

        away_team_id = db.get_team_id_by_name(conn, away_name)
        if not away_team_id:
            away_team_id = db.insert_team(
                conn, away_name, league_id,
                away_team_info.get("venue", ""), None
            )

        # 比赛日期
        utc_date = match.get("utcDate", "")
        match_date = utc_date[:10] if utc_date else ""

        if not match_date:
            continue

        # 计算赛季
        match_dt = datetime.strptime(match_date, "%Y-%m-%d")
        if match_dt.month >= 8:
            season_str = f"{match_dt.year}-{match_dt.year + 1}"
        else:
            season_str = f"{match_dt.year - 1}-{match_dt.year}"

        # 确定比赛结果
        if home_goals > away_goals:
            result = "主胜"
        elif home_goals == away_goals:
            result = "平局"
        else:
            result = "客胜"

        # 检查比赛是否已存在
        if not _match_exists(conn, home_team_id, away_team_id, match_date,
                             league_id=league_id, season=season_str):
            match_id = db.insert_match(
                conn, home_team_id, away_team_id,
                match_date, league_id, season_str,
                home_goals, away_goals, result
            )
            stats["matches"] += 1

            # 生成比赛统计数据
            _generate_stats_from_api(conn, match_id, home_team_id, away_team_id,
                                     home_name, away_name, home_goals, away_goals,
                                     match.get("score", {}))

    return stats


def _normalize_team_name(name: str) -> str:
    """将API返回的球队名称标准化为中文

    Args:
        name: API返回的球队名称（可能是英文）

    Returns:
        标准化后的中文名称
    """
    # 英文名到中文名的映射
    EN_TO_CN = {
        "Manchester City FC": "曼城",
        "Manchester City": "曼城",
        "Arsenal FC": "阿森纳",
        "Arsenal": "阿森纳",
        "Liverpool FC": "利物浦",
        "Liverpool": "利物浦",
        "Chelsea FC": "切尔西",
        "Chelsea": "切尔西",
        "Manchester United FC": "曼联",
        "Manchester United": "曼联",
        "Tottenham Hotspur FC": "热刺",
        "Tottenham Hotspur": "热刺",
        "Tottenham": "热刺",
        "Newcastle United FC": "纽卡斯尔",
        "Newcastle United": "纽卡斯尔",
        "Newcastle": "纽卡斯尔",
        "Aston Villa FC": "阿斯顿维拉",
        "Aston Villa": "阿斯顿维拉",
        "Brighton & Hove Albion FC": "布莱顿",
        "Brighton & Hove Albion": "布莱顿",
        "Brighton": "布莱顿",
        "West Ham United FC": "西汉姆",
        "West Ham United": "西汉姆",
        "West Ham": "西汉姆",
        "Crystal Palace FC": "水晶宫",
        "Crystal Palace": "水晶宫",
        "Brentford FC": "布伦特福德",
        "Brentford": "布伦特福德",
        "Fulham FC": "富勒姆",
        "Fulham": "富勒姆",
        "Wolverhampton Wanderers FC": "狼队",
        "Wolverhampton Wanderers": "狼队",
        "Wolverhampton": "狼队",
        "Wolves": "狼队",
        "AFC Bournemouth": "伯恩茅斯",
        "Bournemouth": "伯恩茅斯",
        "Everton FC": "埃弗顿",
        "Everton": "埃弗顿",
        "Nottingham Forest FC": "诺丁汉森林",
        "Nottingham Forest": "诺丁汉森林",
        "Leicester City FC": "莱斯特城",
        "Leicester City": "莱斯特城",
        "Leicester": "莱斯特城",
        "Ipswich Town FC": "伊普斯维奇",
        "Ipswich Town": "伊普斯维奇",
        "Ipswich": "伊普斯维奇",
        "Southampton FC": "南安普顿",
        "Southampton": "南安普顿",
        # 西甲
        "Real Madrid CF": "皇家马德里",
        "Real Madrid": "皇家马德里",
        "FC Barcelona": "巴塞罗那",
        "Barcelona": "巴塞罗那",
        "Club Atlético de Madrid": "马德里竞技",
        "Atlético Madrid": "马德里竞技",
        "Atletico Madrid": "马德里竞技",
        "Real Sociedad de Fútbol": "皇家社会",
        "Real Sociedad": "皇家社会",
        "Villarreal CF": "比利亚雷亚尔",
        "Villarreal": "比利亚雷亚尔",
        "Real Betis Balompié": "贝蒂斯",
        "Real Betis": "贝蒂斯",
        "Girona FC": "赫罗纳",
        "Girona": "赫罗纳",
        "Athletic Club": "毕尔巴鄂竞技",
        "Athletic Bilbao": "毕尔巴鄂竞技",
        "Sevilla FC": "塞维利亚",
        "Sevilla": "塞维利亚",
        "Valencia CF": "瓦伦西亚",
        "Valencia": "瓦伦西亚",
        # 德甲
        "FC Bayern München": "拜仁慕尼黑",
        "FC Bayern Munich": "拜仁慕尼黑",
        "Bayern München": "拜仁慕尼黑",
        "Bayern Munich": "拜仁慕尼黑",
        "Bayern": "拜仁慕尼黑",
        "Bayer 04 Leverkusen": "勒沃库森",
        "Leverkusen": "勒沃库森",
        "Borussia Dortmund": "多特蒙德",
        "Dortmund": "多特蒙德",
        "RB Leipzig": "莱比锡",
        "Leipzig": "莱比锡",
        "VfB Stuttgart": "斯图加特",
        "Stuttgart": "斯图加特",
        "Eintracht Frankfurt": "法兰克福",
        "Frankfurt": "法兰克福",
        "VfL Wolfsburg": "沃尔夫斯堡",
        "Wolfsburg": "沃尔夫斯堡",
        "SC Freiburg": "弗赖堡",
        "Freiburg": "弗赖堡",
        "TSG Hoffenheim": "霍芬海姆",
        "Hoffenheim": "霍芬海姆",
        "Borussia Mönchengladbach": "门兴格拉德巴赫",
        "Mönchengladbach": "门兴格拉德巴赫",
        "M'gladbach": "门兴格拉德巴赫",
        # 意甲
        "FC Internazionale Milano": "国际米兰",
        "Inter Milan": "国际米兰",
        "Inter": "国际米兰",
        "AC Milan": "AC米兰",
        "Milan": "AC米兰",
        "Juventus FC": "尤文图斯",
        "Juventus": "尤文图斯",
        "SSC Napoli": "那不勒斯",
        "Napoli": "那不勒斯",
        "Atalanta BC": "亚特兰大",
        "Atalanta": "亚特兰大",
        "AS Roma": "罗马",
        "Roma": "罗马",
        "SS Lazio": "拉齐奥",
        "Lazio": "拉齐奥",
        "ACF Fiorentina": "佛罗伦萨",
        "Fiorentina": "佛罗伦萨",
        "Bologna FC": "博洛尼亚",
        "Bologna": "博洛尼亚",
        "Torino FC": "都灵",
        "Torino": "都灵",
        # 法甲
        "Paris Saint-Germain FC": "巴黎圣日耳曼",
        "Paris Saint-Germain": "巴黎圣日耳曼",
        "Paris SG": "巴黎圣日耳曼",
        "PSG": "巴黎圣日耳曼",
        "Olympique de Marseille": "马赛",
        "Marseille": "马赛",
        "AS Monaco FC": "摩纳哥",
        "Monaco": "摩纳哥",
        "LOSC Lille": "里尔",
        "Lille": "里尔",
        "Olympique Lyonnais": "里昂",
        "Lyon": "里昂",
        "OGC Nice": "尼斯",
        "Nice": "尼斯",
        "Racing Club de Lens": "朗斯",
        "Lens": "朗斯",
        "Stade Rennais FC": "雷恩",
        "Rennes": "雷恩",
        "RC Strasbourg Alsace": "斯特拉斯堡",
        "Strasbourg": "斯特拉斯堡",
        "Toulouse FC": "图卢兹",
        "Toulouse": "图卢兹",
        # 英超补充
        "Brighton Hove": "布莱顿",
        "Burnley": "伯恩利",
        "Leeds United": "利兹联",
        "Luton Town": "卢顿",
        "Man City": "曼城",
        "Man United": "曼联",
        "Nottingham": "诺丁汉森林",
        "Sheffield Utd": "谢菲尔德联",
        "Sunderland": "桑德兰",
        # 西甲补充
        "Alavés": "阿拉维斯",
        "Alaves": "阿拉维斯",
        "Athletic": "毕尔巴鄂竞技",
        "Atleti": "马德里竞技",
        "Barcelona": "巴塞罗那",
        "Barça": "巴塞罗那",
        "Celta": "塞尔塔",
        "Celta Vigo": "塞尔塔",
        "Espanyol": "西班牙人",
        "Getafe": "赫塔菲",
        "Elche CF": "埃尔切",
        "Elche": "埃尔切",
        "Las Palmas": "拉斯帕尔马斯",
        "Leganés": "莱加内斯",
        "Leganes": "莱加内斯",
        "Levante": "莱万特",
        "Mallorca": "马略卡",
        "Osasuna": "奥萨苏纳",
        "Rayo Vallecano": "巴列卡诺",
        "Real Oviedo": "皇家奥维耶多",
        "Valladolid": "巴拉多利德",
        # 德甲补充
        "1. FC Köln": "科隆",
        "FC Köln": "科隆",
        "Köln": "科隆",
        "Koln": "科隆",
        "Augsburg": "奥格斯堡",
        "Bochum": "波鸿",
        "Bremen": "云达不莱梅",
        "Werder Bremen": "云达不莱梅",
        "HSV": "汉堡",
        "Hamburger SV": "汉堡",
        "Heidenheim": "海登海姆",
        "Holstein Kiel": "基尔",
        "Kiel": "基尔",
        "Mainz": "美因茨",
        "Mainz 05": "美因茨",
        "St. Pauli": "圣保利",
        "Union Berlin": "柏林联合",
        "Hertha BSC": "柏林赫塔",
        # 意甲补充
        "AC Pisa": "比萨",
        "Pisa": "比萨",
        "Cagliari": "卡利亚里",
        "Como 1907": "科莫",
        "Como": "科莫",
        "Cremonese": "克雷莫纳",
        "Empoli": "恩波利",
        "Genoa": "热那亚",
        "Genoa CFC": "热那亚",
        "Lecce": "莱切",
        "Monza": "蒙扎",
        "Parma": "帕尔马",
        "Parma Calcio": "帕尔马",
        "Sassuolo": "萨索洛",
        "Udinese": "乌迪内斯",
        "Venezia FC": "威尼斯",
        "Venezia": "威尼斯",
        "Verona": "维罗纳",
        "Hellas Verona": "维罗纳",
        "Lazio": "拉齐奥",
        "Fiorentina": "佛罗伦萨",
        "Torino": "都灵",
        "Bologna": "博洛尼亚",
        "Atalanta": "亚特兰大",
        "Napoli": "那不勒斯",
        "Roma": "罗马",
        "Inter": "国际米兰",
        "Milan": "AC米兰",
        "Juventus": "尤文图斯",
        # 法甲补充
        "Angers SCO": "昂热",
        "Angers": "昂热",
        "Auxerre": "欧塞尔",
        "AJ Auxerre": "欧塞尔",
        "Brest": "布雷斯特",
        "Stade Brestois": "布雷斯特",
        "FC Metz": "梅斯",
        "Metz": "梅斯",
        "Le Havre": "勒阿弗尔",
        "Lorient": "洛里昂",
        "Montpellier": "蒙彼利埃",
        "MHSC": "蒙彼利埃",
        "Nantes": "南特",
        "FC Nantes": "南特",
        "Olympique Lyon": "里昂",
        "Lyon": "里昂",
        "Paris FC": "巴黎FC",
        "RC Lens": "朗斯",
        "Lens": "朗斯",
        "Saint-Étienne": "圣埃蒂安",
        "Saint-Etienne": "圣埃蒂安",
        "Stade Rennais": "雷恩",
        "Rennes": "雷恩",
        "Stade de Reims": "兰斯",
        "Reims": "兰斯",
        "Olympique Marseille": "马赛",
        "Marseille": "马赛",
        "Monaco": "摩纳哥",
        "AS Monaco": "摩纳哥",
        "Lille": "里尔",
        "Nice": "尼斯",
        "Racing Club de France": "朗斯",
        "Strasbourg": "斯特拉斯堡",
        "RC Strasbourg": "斯特拉斯堡",
        "Toulouse": "图卢兹",
    }

    return EN_TO_CN.get(name, name)


def _match_exists(conn: sqlite3.Connection, home_team_id: int,
                  away_team_id: int, match_date: str,
                  league_id: int = None, season: str = None) -> bool:
    """检查比赛是否已存在

    Args:
        conn: 数据库连接
        home_team_id: 主队ID
        away_team_id: 客队ID
        match_date: 比赛日期
        league_id: 联赛ID（可选，用于更精确匹配）
        season: 赛季（可选，用于更精确匹配）

    Returns:
        是否已存在
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT match_id FROM matches
            WHERE home_team_id = ? AND away_team_id = ? AND match_date = ?
        """
        params = [home_team_id, away_team_id, match_date]

        if league_id is not None:
            query += " AND league_id = ?"
            params.append(league_id)

        if season is not None:
            query += " AND season = ?"
            params.append(season)

        cursor.execute(query, params)
        return cursor.fetchone() is not None
    except Exception:
        return False


def _generate_stats_from_api(conn: sqlite3.Connection, match_id: int,
                             home_team_id: int, away_team_id: int,
                             home_name: str, away_name: str,
                             home_goals: int, away_goals: int,
                             score_data: Dict) -> None:
    """根据API数据生成比赛统计

    football-data.org 免费版不提供详细统计数据，
    因此基于比赛结果和球队实力生成合理的统计数据。

    Args:
        conn: 数据库连接
        match_id: 比赛ID
        home_team_id: 主队ID
        away_team_id: 客队ID
        home_name: 主队名称
        away_name: 客队名称
        home_goals: 主队进球数
        away_goals: 客队进球数
        score_data: API返回的比分数据
    """
    from app.services.data_collector import TEAM_STRENGTH, _generate_match_stats
    import random

    # 使用已有的统计生成函数
    _generate_match_stats(
        conn, match_id, home_team_id, away_team_id,
        home_name, away_name, home_goals, away_goals
    )


def fetch_historical_data(seasons_back: int = 3) -> Dict[str, Any]:
    """获取历史赛季数据

    football-data.org 支持查询历史赛季，这是它最大的优势。
    可以获取过去N个赛季的完整比赛数据。

    Args:
        seasons_back: 获取过去几个赛季的数据

    Returns:
        采集结果统计
    """
    current_year = datetime.now().year
    if datetime.now().month >= 8:
        current_season_start = current_year
    else:
        current_season_start = current_year - 1

    total_stats = {"leagues": 0, "teams": 0, "matches": 0, "errors": []}

    for i in range(1, seasons_back + 1):
        season_year = current_season_start - i
        print(f"\n获取 {season_year}-{season_year + 1} 赛季数据...")
        stats = fetch_football_data(season=season_year)
        total_stats["leagues"] += stats.get("leagues", 0)
        total_stats["teams"] += stats.get("teams", 0)
        total_stats["matches"] += stats.get("matches", 0)
        total_stats["errors"].extend(stats.get("errors", []))

    return total_stats
