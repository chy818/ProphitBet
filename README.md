# ProphitBet 竞彩预测系统

基于因子化监督学习的足球比赛概率预测平台。

## 项目简介

ProphitBet 是一个完整的足球比赛预测系统，采用 Python 全栈架构（Streamlit + FastAPI + SQLite + scikit-learn/XGBoost），实现了：

- **13类45+因子体系**：进攻端、防守端、交互、状态趋势、交锋历史、赛程情景、稳定性、实力底蕴、统治力、对阵档位、战意压力、球员缺阵、疲劳指数
- **双模型预测**：胜负预测（逻辑回归+XGBoost）+ 进球数预测（XGBoost回归+双泊松分布）
- **可视化展示**：胜负概率饼图、Top5比分柱状图、大小球概率、预期进球对比

## 技术栈

| 层级 | 技术 |
|------|------|
| 前端 | Streamlit、Plotly |
| 后端 | FastAPI、Uvicorn |
| 数据库 | SQLite |
| 机器学习 | scikit-learn、XGBoost、SciPy |
| 数据处理 | Pandas、NumPy |

## 项目结构

```
ProphitBet/
├── app/                          # 后端核心代码
│   ├── config.py                 # 全局配置
│   ├── database.py               # 数据库（6张表+CRUD）
│   ├── main.py                  # FastAPI入口
│   ├── api/                     # API路由
│   ├── models/                   # Pydantic数据模型
│   ├── services/                 # 业务逻辑
│   └── ml/                      # 机器学习模型
├── frontend/                     # Streamlit前端
│   ├── app.py                   # 主页
│   └── pages/                   # 页面
├── data/                        # 数据存储
├── tests/                       # 测试
├── requirements.txt              # 依赖清单
└── README.md
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 启动前端

```bash
cd ProphitBet
streamlit run frontend/app.py --server.port 8501
```

访问 http://localhost:8501

### 3. 启动后端API（可选）

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

API文档：http://localhost:8000/docs

## 功能说明

### 比赛预测
- 选择联赛、主队、客队
- 查看胜负概率（主胜/平局/客胜）
- 查看预期进球数、最可能比分
- 查看大小球概率、两队都进球概率
- 因子分析详情

### 数据分析
- 联赛积分榜（5大联赛）
- 球队数据对比（雷达图）
- 数据分布概览

### 历史验证
- 模型性能指标
- 历史预测准确率统计
- 模型重新训练

## 数据说明

系统内置示例数据，包含：
- **5大联赛**：英超、西甲、德甲、意甲、法甲
- **60支球队**：各联赛顶级球队
- **3个赛季**：模拟历史数据
- **2220场比赛**：完整的比赛和统计数据

## 因子体系

| 类别 | 因子 |
|------|------|
| 进攻端 | 场均预期进球、射正次数、关键传球 |
| 防守端 | 场均预期失球、被射正次数、压迫强度 |
| 交互 | 进攻/防守对比因子 |
| 状态趋势 | 加权近期表现、实际vs预期积分 |
| 交锋历史 | 近6场交锋胜率、进球/失球 |
| 赛程情景 | 休息天数差、主场优势 |
| 稳定性 | 预期进球/失球方差 |
| 实力底蕴 | 联赛排名差、历史排名差 |
| 统治力 | 净胜球差、胜率差 |
| 对阵档位 | 对阵上下半区表现 |
| 战意压力 | 欧战区分差、降级区分差 |
| 球员缺阵 | 核心球员缺阵影响 |
| 疲劳指数 | 基于赛程密度的疲劳度 |

## 预测模型

### 胜负预测
- **逻辑回归**：基线模型，多分类
- **XGBoost**：主预测模型，200棵树，深度6

### 进球数预测
- **XGBoost回归**：分别预测主客队进球数
- **双泊松分布**：基于预期进球计算比分概率矩阵

## API接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/predict` | POST | 预测比赛结果 |
| `/api/teams` | GET | 获取球队列表 |
| `/api/leagues` | GET | 获取联赛列表 |
| `/api/matches` | GET | 获取比赛列表 |
| `/api/factors/calculate` | GET | 计算比赛因子 |

## 配置说明

主要配置项（`app/config.py`）：

```python
DATABASE_PATH = "data/prophitbet.db"  # 数据库路径
MODEL_DIR = "data/models"             # 模型存储路径
RECENT_MATCHES_WINDOW = 5             # 近期比赛窗口
HEAD_TO_HEAD_K = 6                    # 交锋记录数量
OVER_UNDER_THRESHOLD = 2.5           # 大小球阈值
```

## 注意事项

1. 示例数据为模拟生成，用于演示和开发测试
2. 真实预测需要接入真实的比赛数据和统计接口
3. 模型预测仅供参考，不构成投注建议

## License

MIT License
