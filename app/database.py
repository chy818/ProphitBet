"""数据库模块，负责SQLite数据库的初始化、连接管理和CRUD操作"""
import sqlite3
import os
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

from app.config import DATABASE_PATH, MODEL_DIR


def get_connection() -> sqlite3.Connection:
    """获取数据库连接，启用外键约束和行工厂"""
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_db():
    """数据库连接上下文管理器，自动提交和关闭连接"""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_database():
    """初始化数据库，创建所有表结构"""
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    os.makedirs(MODEL_DIR, exist_ok=True)

    with get_db() as conn:
        cursor = conn.cursor()

        # 创建联赛表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leagues (
                league_id INTEGER PRIMARY KEY,
                league_name VARCHAR(100) NOT NULL,
                country VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 创建球队表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS teams (
                team_id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_name VARCHAR(100) NOT NULL,
                league_id INTEGER NOT NULL,
                home_ground VARCHAR(100),
                founded_year INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (league_id) REFERENCES leagues(league_id)
            )
        """)

        # 创建比赛表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL,
                match_date DATE NOT NULL,
                league_id INTEGER NOT NULL,
                season VARCHAR(20) NOT NULL,
                home_goals INTEGER,
                away_goals INTEGER,
                result VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
                FOREIGN KEY (away_team_id) REFERENCES teams(team_id),
                FOREIGN KEY (league_id) REFERENCES leagues(league_id)
            )
        """)

        # 创建比赛统计表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS match_stats (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                team_id INTEGER NOT NULL,
                expected_goals FLOAT,
                shots_on_target INTEGER,
                key_passes INTEGER,
                shots INTEGER,
                attacking_third_entries INTEGER,
                set_pieces_shots INTEGER,
                expected_goals_conceded FLOAT,
                shots_on_target_conceded INTEGER,
                pressing_intensity INTEGER,
                aerial_duel_success FLOAT,
                goalkeeper_save_rate FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches(match_id),
                FOREIGN KEY (team_id) REFERENCES teams(team_id),
                UNIQUE(match_id, team_id)
            )
        """)

        # 创建因子表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS factors (
                factor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                team_id INTEGER NOT NULL,
                factor_type VARCHAR(50) NOT NULL,
                factor_value FLOAT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches(match_id),
                FOREIGN KEY (team_id) REFERENCES teams(team_id)
            )
        """)

        # 创建预测结果表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER NOT NULL,
                home_win_prob FLOAT NOT NULL,
                draw_prob FLOAT NOT NULL,
                away_win_prob FLOAT NOT NULL,
                home_expected_goals FLOAT NOT NULL,
                away_expected_goals FLOAT NOT NULL,
                most_likely_score VARCHAR(10) NOT NULL,
                over_25_prob FLOAT,
                both_teams_score_prob FLOAT,
                model_version VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (match_id) REFERENCES matches(match_id)
            )
        """)

        # 创建索引以提升查询性能
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_home_team ON matches(home_team_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_away_team ON matches(away_team_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_matches_league ON matches(league_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_match_stats_match ON match_stats(match_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_factors_match ON factors(match_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_factors_type ON factors(factor_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_predictions_match ON predictions(match_id)")

        conn.commit()


# ==================== 球队 CRUD ====================

def insert_team(conn: sqlite3.Connection, team_name: str, league_id: int,
                home_ground: Optional[str] = None, founded_year: Optional[int] = None) -> int:
    """插入球队记录，返回球队ID"""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO teams (team_name, league_id, home_ground, founded_year) VALUES (?, ?, ?, ?)",
        (team_name, league_id, home_ground, founded_year)
    )
    return cursor.lastrowid


def get_team_by_name(conn: sqlite3.Connection, team_name: str) -> Optional[Dict[str, Any]]:
    """根据球队名称查询球队信息"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM teams WHERE team_name = ?", (team_name,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_all_teams(conn: sqlite3.Connection, league_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """获取所有球队，可按联赛ID筛选"""
    cursor = conn.cursor()
    if league_id:
        cursor.execute("SELECT * FROM teams WHERE league_id = ? ORDER BY team_name", (league_id,))
    else:
        cursor.execute("SELECT * FROM teams ORDER BY team_name")
    return [dict(row) for row in cursor.fetchall()]


# ==================== 比赛 CRUD ====================

def insert_match(conn: sqlite3.Connection, home_team_id: int, away_team_id: int,
                 match_date: str, league_id: int, season: str,
                 home_goals: Optional[int] = None, away_goals: Optional[int] = None,
                 result: Optional[str] = None) -> int:
    """插入比赛记录，返回比赛ID"""
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO matches (home_team_id, away_team_id, match_date, league_id, season,
           home_goals, away_goals, result) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (home_team_id, away_team_id, match_date, league_id, season, home_goals, away_goals, result)
    )
    return cursor.lastrowid


def get_matches_by_team(conn: sqlite3.Connection, team_id: int,
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """获取某球队参与的所有比赛"""
    cursor = conn.cursor()
    query = """
        SELECT m.*, ht.team_name as home_team_name, at.team_name as away_team_name
        FROM matches m
        JOIN teams ht ON m.home_team_id = ht.team_id
        JOIN teams at ON m.away_team_id = at.team_id
        WHERE m.home_team_id = ? OR m.away_team_id = ?
        ORDER BY m.match_date DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query, (team_id, team_id))
    return [dict(row) for row in cursor.fetchall()]


def get_head_to_head_matches(conn: sqlite3.Connection, team_a_id: int, team_b_id: int,
                             limit: int = 6) -> List[Dict[str, Any]]:
    """获取两队交锋历史记录"""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT m.*, ht.team_name as home_team_name, at.team_name as away_team_name
           FROM matches m
           JOIN teams ht ON m.home_team_id = ht.team_id
           JOIN teams at ON m.away_team_id = at.team_id
           WHERE (m.home_team_id = ? AND m.away_team_id = ?)
              OR (m.home_team_id = ? AND m.away_team_id = ?)
           ORDER BY m.match_date DESC LIMIT ?""",
        (team_a_id, team_b_id, team_b_id, team_a_id, limit)
    )
    return [dict(row) for row in cursor.fetchall()]


# ==================== 比赛统计 CRUD ====================

def insert_match_stats(conn: sqlite3.Connection, match_id: int, team_id: int,
                       stats: Dict[str, Any]) -> int:
    """插入比赛统计记录"""
    cursor = conn.cursor()
    cursor.execute(
        """INSERT OR REPLACE INTO match_stats
           (match_id, team_id, expected_goals, shots_on_target, key_passes, shots,
            attacking_third_entries, set_pieces_shots, expected_goals_conceded,
            shots_on_target_conceded, pressing_intensity, aerial_duel_success,
            goalkeeper_save_rate)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (match_id, team_id,
         stats.get("expected_goals"), stats.get("shots_on_target"),
         stats.get("key_passes"), stats.get("shots"),
         stats.get("attacking_third_entries"), stats.get("set_pieces_shots"),
         stats.get("expected_goals_conceded"), stats.get("shots_on_target_conceded"),
         stats.get("pressing_intensity"), stats.get("aerial_duel_success"),
         stats.get("goalkeeper_save_rate"))
    )
    return cursor.lastrowid


def get_match_stats(conn: sqlite3.Connection, match_id: int) -> List[Dict[str, Any]]:
    """获取某场比赛的统计数据"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM match_stats WHERE match_id = ?", (match_id,))
    return [dict(row) for row in cursor.fetchall()]


def get_team_stats_history(conn: sqlite3.Connection, team_id: int,
                           limit: int = 10) -> List[Dict[str, Any]]:
    """获取球队最近的比赛统计数据"""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT ms.* FROM match_stats ms
           JOIN matches m ON ms.match_id = m.match_id
           WHERE ms.team_id = ?
           ORDER BY m.match_date DESC LIMIT ?""",
        (team_id, limit)
    )
    return [dict(row) for row in cursor.fetchall()]


# ==================== 因子 CRUD ====================

def insert_factors(conn: sqlite3.Connection, match_id: int, team_id: int,
                   factors: Dict[str, float]) -> List[int]:
    """批量插入因子记录"""
    cursor = conn.cursor()
    ids = []
    for factor_type, factor_value in factors.items():
        cursor.execute(
            "INSERT INTO factors (match_id, team_id, factor_type, factor_value) VALUES (?, ?, ?, ?)",
            (match_id, team_id, factor_type, factor_value)
        )
        ids.append(cursor.lastrowid)
    return ids


def get_factors_by_match(conn: sqlite3.Connection, match_id: int) -> List[Dict[str, Any]]:
    """获取某场比赛的所有因子"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM factors WHERE match_id = ?", (match_id,))
    return [dict(row) for row in cursor.fetchall()]


# ==================== 预测结果 CRUD ====================

def insert_prediction(conn: sqlite3.Connection, match_id: int,
                      home_win_prob: float, draw_prob: float, away_win_prob: float,
                      home_expected_goals: float, away_expected_goals: float,
                      most_likely_score: str, over_25_prob: Optional[float] = None,
                      both_teams_score_prob: Optional[float] = None,
                      model_version: Optional[str] = None) -> int:
    """插入预测结果记录"""
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO predictions (match_id, home_win_prob, draw_prob, away_win_prob,
           home_expected_goals, away_expected_goals, most_likely_score,
           over_25_prob, both_teams_score_prob, model_version)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (match_id, home_win_prob, draw_prob, away_win_prob,
         home_expected_goals, away_expected_goals, most_likely_score,
         over_25_prob, both_teams_score_prob, model_version)
    )
    return cursor.lastrowid


def get_prediction_by_match(conn: sqlite3.Connection, match_id: int) -> Optional[Dict[str, Any]]:
    """获取某场比赛的预测结果"""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM predictions WHERE match_id = ? ORDER BY created_at DESC LIMIT 1",
        (match_id,)
    )
    row = cursor.fetchone()
    return dict(row) if row else None


# ==================== 联赛 CRUD ====================

def insert_league(conn: sqlite3.Connection, league_id: int, league_name: str,
                  country: Optional[str] = None) -> int:
    """插入联赛记录"""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR IGNORE INTO leagues (league_id, league_name, country) VALUES (?, ?, ?)",
        (league_id, league_name, country)
    )
    return league_id


def get_all_leagues(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """获取所有联赛"""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM leagues ORDER BY league_id")
    return [dict(row) for row in cursor.fetchall()]
