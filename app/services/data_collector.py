"""数据采集模块，负责从外部数据源获取比赛数据并生成示例数据"""
import random
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd

from app.config import SUPPORTED_LEAGUES, RANDOM_SEED
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
    {"name": "雷恩", "home_ground": "洛里安路球场", "founded_year": 1901},
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
    """生成示例数据，包括联赛、球队、比赛和统计数据

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
                team_id = db.insert_team(
                    conn, team_info["name"], league_id,
                    team_info["home_ground"], team_info["founded_year"]
                )
                team_id_map[team_info["name"]] = team_id

        # 为每个赛季生成比赛数据
        current_year = datetime.now().year
        for season_offset in range(seasons):
            season_year = current_year - seasons + season_offset + 1
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
    """从外部API获取数据（预留接口）

    Args:
        api_key: API密钥，应从环境变量获取，不应硬编码
    """
    pass
