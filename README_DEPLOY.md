# ProphitBet 云端部署指南

## 概述

本项目可部署到云平台，实现多人通过网页访问使用。

## 推荐部署方案

### 1. Streamlit Cloud（推荐 - 完全免费）

#### 前置条件
- GitHub 账号
- 代码已上传到 GitHub 仓库（公开仓库）

#### 部署步骤

**Step 1: 准备代码**

1. 确保代码已推送到 GitHub
2. 代码中不能包含 `.env` 文件（已通过 .gitignore 排除）
3. API 密钥将通过云平台的安全配置添加

**Step 2: 创建新仓库（如需要）**

```bash
# 在项目目录下初始化 Git（如果还没有）
git init
git add .
git commit -m "Deploy to Streamlit Cloud"

# 推送到 GitHub（替换为你的仓库地址）
git remote add origin https://github.com/你的用户名/ProphitBet.git
git push -u origin main
```

**Step 3: 在 Streamlit Cloud 部署**

1. 访问 [Streamlit Cloud](https://streamlit.io/cloud)
2. 点击 "Sign in" 使用 GitHub 账号登录
3. 点击 "New app"
4. 填写配置：
   - **Repository**: 选择你的仓库（如 `你的用户名/ProphitBet`）
   - **Branch**: `main`
   - **Main file path**: `frontend/app.py`
5. 点击 "Deploy!"

**Step 4: 配置 API 密钥（Secrets）**

1. 部署完成后，在 app 页面点击 "Manage app"
2. 点击 "Settings" → "Secrets"
3. 添加以下密钥：

```toml
# football-data.org API Token
FOOTBALL_DATA_API_TOKEN = "你的API密钥"

# 聚合数据 API Key（如使用）
JUHE_API_KEY = "你的API密钥"
```

4. 点击 "Save"
5. 应用将自动重启

**Step 5: 访问应用**

部署成功后，通过以下地址访问：
```
https://你的用户名-prophitbet-streamlit-app-main-你的ID.streamlit.app
```

---

### 2. HuggingFace Spaces（免费 - 推荐国内用户）

#### 部署步骤

1. 访问 [HuggingFace Spaces](https://huggingface.co/new-space)
2. 选择 "Streamlit" 作为 SDK
3. 创建新 Space，填写名称（如 `prophitbet`）
4. 上传代码或连接 GitHub 仓库
5. 在 Space 的 "Files" 页面创建 `.env` 文件存储密钥
6. 访问你的 Space 地址

---

### 3. Railway（付费但稳定）

适合需要更稳定服务的场景。

---

## 云端数据存储说明

### 问题

云平台（如 Streamlit Cloud）使用临时文件系统，**重启后数据会丢失**。

### 解决方案

#### 方案 A: 每次启动时从 API 拉取数据（推荐）

- 云端部署后，每次启动时自动从 football-data.org 获取最新数据
- 不存储本地数据库，所有数据实时从 API 获取
- 适合数据展示和预测

#### 方案 B: 使用云数据库

- 切换到云数据库（如 SQLite on cloud、PostgreSQL）
- 需要额外的数据库配置

#### 方案 C: 定期同步

- 设置定时任务从本地数据库同步到云端
- 较复杂，不推荐

---

## 功能限制（云端部署）

| 功能 | 本地部署 | 云端部署 |
|------|----------|----------|
| 数据采集 | ✅ 完整支持 | ✅ 从API获取 |
| 历史数据存储 | ✅ 本地SQLite | ⚠️ 临时存储 |
| 因子手动调整 | ✅ 完整支持 | ⚠️ 重启后丢失 |
| 模型训练 | ✅ 完整支持 | ⚠️ 重启后丢失 |
| 多用户访问 | ❌ 不支持 | ✅ 支持 |

---

## 故障排除

### 部署失败

1. 检查 `requirements.txt` 是否包含所有依赖
2. 检查 GitHub 仓库是否为公开仓库
3. 查看 Streamlit Cloud 的部署日志

### API 密钥无效

1. 确认密钥已正确添加到 Secrets
2. 检查密钥格式（不要有引号）
3. 重启应用

### 数据不显示

1. 首次部署需要等待数据从 API 加载
2. 检查 API 配额是否用完
3. 刷新页面

---

## 相关链接

- [Streamlit Cloud 文档](https://docs.streamlit.io/streamlit-cloud)
- [football-data.org API](https://www.football-data.org/)
- [聚合数据 API](https://www.juhe.cn/docs/api/id/235)

---

## 联系方式

如有问题，请提交 GitHub Issue。
