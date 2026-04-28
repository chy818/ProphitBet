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

        # 创建因子调整表（用于手动覆盖因子值）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS factor_adjustments (
                adjustment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                team_id INTEGER NOT NULL,
                factor_name VARCHAR(100) NOT NULL,
                factor_category VARCHAR(50) NOT NULL,
                adjusted_value FLOAT NOT NULL,
                original_value FLOAT,
                reason TEXT,
                effective_from DATE DEFAULT (date('now')),
                effective_to DATE,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (team_id) REFERENCES teams(team_id)
            )
        """)

        # 创建因子开关表（用于全局控制因子的启用/禁用）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS factor_switches (
                switch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                factor_name VARCHAR(100) NOT NULL UNIQUE,
                factor_category VARCHAR(50) NOT NULL,
                is_enabled BOOLEAN DEFAULT 1,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

        # 因子调整表索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_factor_adjustments_team ON factor_adjustments(team_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_factor_adjustments_active ON factor_adjustments(is_active)")

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


# ==================== 因子调整 CRUD ====================

def insert_factor_adjustment(conn: sqlite3.Connection, team_id: int,
                              factor_name: str, factor_category: str,
                              adjusted_value: float, original_value: Optional[float] = None,
                              reason: Optional[str] = None,
                              effective_from: Optional[str] = None,
                              effective_to: Optional[str] = None) -> int:
    """插入因子调整记录"""
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO factor_adjustments
           (team_id, factor_name, factor_category, adjusted_value, original_value,
            reason, effective_from, effective_to)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (team_id, factor_name, factor_category, adjusted_value, original_value,
         reason, effective_from, effective_to)
    )
    return cursor.lastrowid


def get_active_factor_adjustments(conn: sqlite3.Connection,
                                   team_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """获取当前生效的因子调整，可按球队筛选"""
    cursor = conn.cursor()
    if team_id:
        cursor.execute(
            """SELECT fa.*, t.team_name
               FROM factor_adjustments fa
               JOIN teams t ON fa.team_id = t.team_id
               WHERE fa.is_active = 1
               AND (fa.effective_from IS NULL OR fa.effective_from <= date('now'))
               AND (fa.effective_to IS NULL OR fa.effective_to >= date('now'))
               AND fa.team_id = ?
               ORDER BY fa.factor_category, fa.factor_name""",
            (team_id,)
        )
    else:
        cursor.execute(
            """SELECT fa.*, t.team_name
               FROM factor_adjustments fa
               JOIN teams t ON fa.team_id = t.team_id
               WHERE fa.is_active = 1
               AND (fa.effective_from IS NULL OR fa.effective_from <= date('now'))
               AND (fa.effective_to IS NULL OR fa.effective_to >= date('now'))
               ORDER BY t.team_name, fa.factor_category, fa.factor_name"""
        )
    return [dict(row) for row in cursor.fetchall()]


def get_all_factor_adjustments(conn: sqlite3.Connection,
                                 team_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """获取所有因子调整记录（包括已失效的）"""
    cursor = conn.cursor()
    if team_id:
        cursor.execute(
            """SELECT fa.*, t.team_name
               FROM factor_adjustments fa
               JOIN teams t ON fa.team_id = t.team_id
               WHERE fa.team_id = ?
               ORDER BY fa.created_at DESC""",
            (team_id,)
        )
    else:
        cursor.execute(
            """SELECT fa.*, t.team_name
               FROM factor_adjustments fa
               JOIN teams t ON fa.team_id = t.team_id
               ORDER BY fa.created_at DESC"""
        )
    return [dict(row) for row in cursor.fetchall()]


def update_factor_adjustment(conn: sqlite3.Connection, adjustment_id: int,
                               adjusted_value: float, reason: Optional[str] = None) -> bool:
    """更新因子调整值"""
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE factor_adjustments
           SET adjusted_value = ?, reason = ?, updated_at = CURRENT_TIMESTAMP
           WHERE adjustment_id = ?""",
        (adjusted_value, reason, adjustment_id)
    )
    return cursor.rowcount > 0


def deactivate_factor_adjustment(conn: sqlite3.Connection, adjustment_id: int) -> bool:
    """停用因子调整"""
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE factor_adjustments
           SET is_active = 0, updated_at = CURRENT_TIMESTAMP
           WHERE adjustment_id = ?""",
        (adjustment_id,)
    )
    return cursor.rowcount > 0


def delete_factor_adjustment(conn: sqlite3.Connection, adjustment_id: int) -> bool:
    """删除因子调整记录"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM factor_adjustments WHERE adjustment_id = ?",
                   (adjustment_id,))
    return cursor.rowcount > 0


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


def get_league_id_by_name(conn: sqlite3.Connection, league_name: str) -> Optional[int]:
    """根据联赛名称获取联赛ID"""
    cursor = conn.cursor()
    cursor.execute("SELECT league_id FROM leagues WHERE league_name = ?", (league_name,))
    row = cursor.fetchone()
    return row[0] if row else None


def get_team_id_by_name(conn: sqlite3.Connection, team_name: str) -> Optional[int]:
    """根据球队名称获取球队ID"""
    cursor = conn.cursor()
    cursor.execute("SELECT team_id FROM teams WHERE team_name = ?", (team_name,))
    row = cursor.fetchone()
    return row[0] if row else None


# ==================== 因子开关 CRUD ====================

def init_factor_switches(conn: sqlite3.Connection, factor_list: List[Dict[str, str]]) -> None:
    """初始化因子开关，确保所有因子都有对应的开关记录"""
    cursor = conn.cursor()
    for factor in factor_list:
        cursor.execute(
            """INSERT OR IGNORE INTO factor_switches
               (factor_name, factor_category, is_enabled, description)
               VALUES (?, ?, ?, ?)""",
            (factor["name"], factor["category"], True, factor.get("description", ""))
        )
    conn.commit()


def get_all_factor_switches(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """获取所有因子开关状态"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM factor_switches
        ORDER BY factor_category, factor_name
    """)
    return [dict(row) for row in cursor.fetchall()]


def get_enabled_factor_switches(conn: sqlite3.Connection) -> List[str]:
    """获取所有启用的因子名称"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT factor_name FROM factor_switches
        WHERE is_enabled = 1
    """)
    return [row["factor_name"] for row in cursor.fetchall()]


def update_factor_switch(conn: sqlite3.Connection, factor_name: str, is_enabled: bool) -> bool:
    """更新因子开关状态"""
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE factor_switches
           SET is_enabled = ?, updated_at = CURRENT_TIMESTAMP
           WHERE factor_name = ?""",
        (is_enabled, factor_name)
    )
    return cursor.rowcount > 0


def toggle_factor_switch(conn: sqlite3.Connection, factor_name: str) -> bool:
    """切换因子开关状态"""
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE factor_switches
           SET is_enabled = NOT is_enabled, updated_at = CURRENT_TIMESTAMP
           WHERE factor_name = ?""",
        (factor_name,)
    )
    return cursor.rowcount > 0


def batch_update_factor_switches(conn: sqlite3.Connection, switches: Dict[str, bool]) -> int:
    """批量更新因子开关状态"""
    cursor = conn.cursor()
    updated_count = 0
    for factor_name, is_enabled in switches.items():
        cursor.execute(
            """UPDATE factor_switches
               SET is_enabled = ?, updated_at = CURRENT_TIMESTAMP
               WHERE factor_name = ?""",
            (is_enabled, factor_name)
        )
        if cursor.rowcount > 0:
            updated_count += 1
    return updated_count


def enable_all_factors(conn: sqlite3.Connection) -> int:
    """启用所有因子"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE factor_switches
        SET is_enabled = 1, updated_at = CURRENT_TIMESTAMP
    """)
    return cursor.rowcount


def disable_all_factors(conn: sqlite3.Connection) -> int:
    """禁用所有因子"""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE factor_switches
        SET is_enabled = 0, updated_at = CURRENT_TIMESTAMP
    """)
    return cursor.rowcount
