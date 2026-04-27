"""FastAPI应用入口，配置中间件、路由和启动事件"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import init_database
from app.api import predict, teams, matches, factors


@asynccontextmanager
async def lifespan(application: FastAPI):
    """应用生命周期管理，启动时初始化数据库"""
    init_database()
    yield


app = FastAPI(
    title="ProphitBet 竞彩预测API",
    description="基于因子化方法的足球比赛预测服务",
    version="1.0.0",
    lifespan=lifespan,
)

# 配置CORS，允许前端跨域访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册API路由
app.include_router(predict.router, prefix="/api", tags=["预测"])
app.include_router(teams.router, prefix="/api", tags=["球队"])
app.include_router(matches.router, prefix="/api", tags=["比赛"])
app.include_router(factors.router, prefix="/api", tags=["因子"])


@app.get("/", summary="健康检查")
async def root():
    """API根路径，返回服务状态"""
    return {"service": "ProphitBet API", "status": "running", "version": "1.0.0"}
