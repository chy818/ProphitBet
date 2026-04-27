"""项目全局配置模块，集中管理所有配置项"""
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 数据库配置
DATABASE_PATH = os.path.join(BASE_DIR, "data", "prophitbet.db")

# 模型存储路径
MODEL_DIR = os.path.join(BASE_DIR, "data", "models")

# API配置
API_HOST = "0.0.0.0"
API_PORT = 8000

# 聚合数据API配置
JUHE_API_KEY = os.getenv("JUHE_API_KEY", "")
JUHE_API_BASE_URL = "https://apis.juhe.cn/fapig/football"

# 数据采集配置
DATA_UPDATE_INTERVAL_HOURS = 24

# 模型训练配置
TRAIN_TEST_SPLIT_RATIO = 0.2
CROSS_VALIDATION_FOLDS = 5
RANDOM_SEED = 42

# 因子计算配置
RECENT_MATCHES_WINDOW = 5
HEAD_TO_HEAD_K = 6
FORM_WEIGHT_DECAY = 0.85

# 联赛配置
SUPPORTED_LEAGUES = {
    1: "英超",
    2: "西甲",
    3: "德甲",
    4: "意甲",
    5: "法甲",
}

# 聚合数据API联赛映射
JUHE_LEAGUE_MAP = {
    "英超": "yingchao",
    "西甲": "xijia",
    "德甲": "dejia",
    "意甲": "yijia",
    "法甲": "fajia",
    "中超": "zhongchao",
    "江苏城市足球联赛": "jiangsu",
}

# 比赛结果常量
RESULT_HOME_WIN = "主胜"
RESULT_DRAW = "平局"
RESULT_AWAY_WIN = "客胜"

# 大小球阈值
OVER_UNDER_THRESHOLD = 2.5
