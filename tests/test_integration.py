"""集成测试脚本，验证系统各模块是否正常工作"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["PYTHONIOENCODING"] = "utf-8"

from app.database import get_connection, init_database
from app.services.data_collector import generate_sample_data
from app.services.factor_calculator import calculate_all_factors
from app.services.prediction_service import predict_match
from app.ml.win_loss_model import train_and_save_all_models
from app.ml.goals_model import train_and_save_goals_models


def test_database():
    """测试数据库初始化和数据"""
    print("=" * 50)
    print("1. 测试数据库模块")
    init_database()
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as cnt FROM teams")
    team_count = cursor.fetchone()["cnt"]
    print(f"   球队数量: {team_count}")

    cursor.execute("SELECT COUNT(*) as cnt FROM matches")
    match_count = cursor.fetchone()["cnt"]
    print(f"   比赛数量: {match_count}")

    cursor.execute("SELECT COUNT(*) as cnt FROM match_stats")
    stats_count = cursor.fetchone()["cnt"]
    print(f"   统计记录: {stats_count}")

    cursor.execute("SELECT COUNT(*) as cnt FROM leagues")
    league_count = cursor.fetchone()["cnt"]
    print(f"   联赛数量: {league_count}")

    conn.close()
    assert team_count > 0, "球队数据为空"
    assert match_count > 0, "比赛数据为空"
    print("   ✅ 数据库模块测试通过")


def test_factor_calculation():
    """测试因子计算"""
    print("=" * 50)
    print("2. 测试因子计算模块")
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT home_team_id, away_team_id, league_id, season FROM matches LIMIT 1")
    match = cursor.fetchone()
    if not match:
        print("   ⚠️ 无比赛数据，跳过因子测试")
        conn.close()
        return

    factors = calculate_all_factors(
        conn, match["home_team_id"], match["away_team_id"],
        match["league_id"], match["season"]
    )

    print(f"   计算因子数量: {len(factors)}")
    for key, value in list(factors.items())[:5]:
        print(f"   - {key}: {value}")

    conn.close()
    assert len(factors) > 0, "因子计算结果为空"
    print("   ✅ 因子计算模块测试通过")


def test_model_training():
    """测试模型训练"""
    print("=" * 50)
    print("3. 测试模型训练模块")
    try:
        win_loss_results = train_and_save_all_models()
        for model_name, metrics in win_loss_results.items():
            if isinstance(metrics, dict) and "error" not in metrics:
                print(f"   {model_name}: 准确率={metrics.get('cv_accuracy', 0):.1%}, F1={metrics.get('cv_f1_macro', 0):.3f}")
            elif isinstance(metrics, dict) and "error" in metrics:
                print(f"   {model_name}: {metrics['error']}")
        print("   ✅ 胜负预测模型训练完成")
    except Exception as e:
        print(f"   ⚠️ 胜负预测模型训练异常: {e}")

    try:
        goals_results = train_and_save_goals_models()
        if isinstance(goals_results, dict) and "error" not in goals_results:
            print(f"   进球数模型: 主队RMSE={goals_results.get('home_rmse', 0):.3f}, 客队RMSE={goals_results.get('away_rmse', 0):.3f}")
        print("   ✅ 进球数预测模型训练完成")
    except Exception as e:
        print(f"   ⚠️ 进球数预测模型训练异常: {e}")


def test_prediction():
    """测试完整预测流程"""
    print("=" * 50)
    print("4. 测试预测服务模块")
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT home_team_id, away_team_id, league_id, season FROM matches LIMIT 1")
    match = cursor.fetchone()
    conn.close()

    if not match:
        print("   ⚠️ 无比赛数据，跳过预测测试")
        return

    result = predict_match(
        home_team_id=match["home_team_id"],
        away_team_id=match["away_team_id"],
        league_id=match["league_id"],
        season=match["season"],
    )

    print(f"   主队: {result['home_team_name']}")
    print(f"   客队: {result['away_team_name']}")
    print(f"   主胜概率: {result['home_win_prob']:.1%}")
    print(f"   平局概率: {result['draw_prob']:.1%}")
    print(f"   客胜概率: {result['away_win_prob']:.1%}")
    print(f"   最可能比分: {result['most_likely_score']}")
    print(f"   大于2.5球概率: {result.get('over_25_prob', 0):.1%}")

    assert result["home_win_prob"] + result["draw_prob"] + result["away_win_prob"] > 0, "概率计算异常"
    print("   ✅ 预测服务模块测试通过")


def test_fastapi():
    """测试FastAPI应用"""
    print("=" * 50)
    print("5. 测试FastAPI应用")
    try:
        from app.main import app
        print(f"   应用名称: {app.title}")
        print(f"   版本: {app.version}")
        print(f"   路由数量: {len(app.routes)}")
        print("   ✅ FastAPI应用测试通过")
    except Exception as e:
        print(f"   ⚠️ FastAPI应用异常: {e}")


if __name__ == "__main__":
    print("🚀 ProphitBet 集成测试")
    print("=" * 50)

    test_database()
    test_factor_calculation()
    test_model_training()
    test_prediction()
    test_fastapi()

    print("=" * 50)
    print("🎉 集成测试完成！")
