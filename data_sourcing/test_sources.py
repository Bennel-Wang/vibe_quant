"""
数据源全面测试脚本
每个数据源测试5个样本: A股大盘, 港股大盘, A股板块, A股个股, 港股个股
所有结果记录到 logs/test_results.log
"""
import sys
import os
import datetime
import traceback
import pandas as pd

# 确保项目根目录在path中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import setup_logger, TEST_SAMPLES, LOG_DIR
from validator import validate_dataframe, has_valid_data
import code_mapper

logger = setup_logger("test_sources", "test_results.log")

# 测试参数
END_DATE = datetime.date.today().strftime("%Y%m%d")
START_DATE = (datetime.date.today() - datetime.timedelta(days=60)).strftime("%Y%m%d")

# 所有数据源
SOURCE_CONFIGS = [
    ("tushare", "sources.tushare_source", "TushareSource"),
    ("baostock", "sources.baostock_source", "BaostockSource"),
    ("easyquotation", "sources.easyquotation_source", "EasyquotationSource"),
    ("pytdx", "sources.pytdx_source", "PytdxSource"),
    ("mootdx", "sources.mootdx_source", "MootdxSource"),
]


def test_single_source(source_name, source_class_path, class_name):
    """测试单个数据源的所有样本"""
    results = []
    logger.info(f"\n{'='*60}")
    logger.info(f"开始测试数据源: {source_name}")
    logger.info(f"{'='*60}")

    # 动态导入
    try:
        module = __import__(source_class_path, fromlist=[class_name])
        SourceClass = getattr(module, class_name)
        source = SourceClass()
    except Exception as e:
        logger.error(f"❌ 导入数据源 {source_name} 失败: {e}")
        logger.error(traceback.format_exc())
        for sample_name, code in TEST_SAMPLES.items():
            results.append({
                "source": source_name,
                "sample": sample_name,
                "code": code,
                "status": "IMPORT_FAIL",
                "rows": 0,
                "error": str(e),
            })
        return results

    # 初始化
    try:
        init_ok = source.init()
        if not init_ok:
            logger.warning(f"⚠️ {source_name} 初始化失败（可能缺少配置）")
    except Exception as e:
        logger.error(f"❌ {source_name} 初始化异常: {e}")
        logger.error(traceback.format_exc())
        for sample_name, code in TEST_SAMPLES.items():
            results.append({
                "source": source_name,
                "sample": sample_name,
                "code": code,
                "status": "INIT_FAIL",
                "rows": 0,
                "error": str(e),
            })
        return results

    # 测试每个样本
    for sample_name, code in TEST_SAMPLES.items():
        market = code_mapper.get_market(code)
        logger.info(f"\n--- 测试 {source_name} / {sample_name} ({code}, {market}) ---")

        result_entry = {
            "source": source_name,
            "sample": sample_name,
            "code": code,
            "market": market,
            "status": "UNKNOWN",
            "rows": 0,
            "error": "",
            "date_range": "",
            "sample_data": "",
        }

        # 检查市场支持
        if not source.supports_market(market):
            msg = f"{source_name} 不支持 {market}"
            logger.info(f"⏭️ {msg}")
            result_entry["status"] = "UNSUPPORTED"
            result_entry["error"] = msg
            results.append(result_entry)
            continue

        try:
            df = source.fetch_daily(code, START_DATE, END_DATE)

            if df is None:
                logger.warning(f"❌ {source_name}/{sample_name}: 返回 None")
                result_entry["status"] = "NO_DATA"
                result_entry["error"] = "fetch_daily returned None"

            elif df.empty:
                logger.warning(f"❌ {source_name}/{sample_name}: 空DataFrame")
                result_entry["status"] = "EMPTY"
                result_entry["error"] = "Empty DataFrame"

            elif not has_valid_data(df):
                logger.warning(f"⚠️ {source_name}/{sample_name}: 数据无效(含0/NaN)")
                result_entry["status"] = "INVALID_DATA"
                result_entry["rows"] = len(df)
                valid_mask, _ = validate_dataframe(df)
                result_entry["error"] = f"有效行: {valid_mask.sum()}/{len(df)}"

            else:
                valid_mask, _ = validate_dataframe(df)
                valid_count = valid_mask.sum()
                date_min = df["trade_date"].min()
                date_max = df["trade_date"].max()

                logger.info(f"✅ {source_name}/{sample_name}: {len(df)} 行, "
                           f"有效 {valid_count}/{len(df)}, "
                           f"日期 {date_min}-{date_max}")

                # 打印样本数据
                sample_str = df.tail(3).to_string(index=False)
                logger.info(f"样本数据:\n{sample_str}")

                result_entry["status"] = "SUCCESS"
                result_entry["rows"] = len(df)
                result_entry["date_range"] = f"{date_min}-{date_max}"
                result_entry["sample_data"] = sample_str

        except Exception as e:
            logger.error(f"❌ {source_name}/{sample_name}: 异常 - {e}")
            logger.error(traceback.format_exc())
            result_entry["status"] = "ERROR"
            result_entry["error"] = str(e)

        results.append(result_entry)

    # 清理
    try:
        source.close()
    except Exception:
        pass

    return results


def test_indicators():
    """测试技术指标计算"""
    logger.info(f"\n{'='*60}")
    logger.info(f"测试技术指标计算模块")
    logger.info(f"{'='*60}")

    try:
        from indicators import compute_all_indicators, BACKEND
        import numpy as np

        logger.info(f"指标计算后端: {BACKEND}")

        # 生成测试数据
        np.random.seed(42)
        n = 100
        close = 10.0 + np.cumsum(np.random.randn(n) * 0.1)
        high = close + np.abs(np.random.randn(n) * 0.05)
        low = close - np.abs(np.random.randn(n) * 0.05)
        open_arr = close + np.random.randn(n) * 0.02
        vol = np.abs(np.random.randn(n) * 1000 + 5000)

        result = compute_all_indicators({
            "open": open_arr,
            "high": high,
            "low": low,
            "close": close,
            "vol": vol,
        })

        logger.info(f"计算得到 {len(result)} 个指标:")
        for key, values in result.items():
            valid = np.sum(~np.isnan(values))
            logger.info(f"  {key}: {valid}/{len(values)} 有效值, "
                       f"范围 [{np.nanmin(values):.4f}, {np.nanmax(values):.4f}]")

        logger.info("✅ 技术指标计算测试通过")
        return True

    except Exception as e:
        logger.error(f"❌ 技术指标计算测试失败: {e}")
        logger.error(traceback.format_exc())
        return False


def test_data_manager():
    """测试DataManager集成(含降级逻辑)"""
    logger.info(f"\n{'='*60}")
    logger.info(f"测试 DataManager 集成 (降级逻辑)")
    logger.info(f"{'='*60}")

    try:
        from data_manager import DataManager

        manager = DataManager()

        # 测试一个A股个股
        code = "000001.SZ"
        logger.info(f"集成测试: {code}")
        df = manager.fetch(code, START_DATE, END_DATE)
        if df is not None and not df.empty:
            logger.info(f"✅ DataManager 获取 {code}: {len(df)} 行")
            logger.info(f"   列: {list(df.columns)}")
            logger.info(f"   日期: {df['trade_date'].min()} - {df['trade_date'].max()}")
        else:
            logger.warning(f"⚠️ DataManager 未获取到 {code} 数据")

        manager.close_all()
        return True

    except Exception as e:
        logger.error(f"❌ DataManager 集成测试失败: {e}")
        logger.error(traceback.format_exc())
        return False


def print_summary(all_results):
    """打印测试摘要"""
    summary = "\n" + "=" * 80
    summary += "\n测试结果摘要"
    summary += "\n" + "=" * 80

    # 按数据源分组
    sources = {}
    for r in all_results:
        src = r["source"]
        if src not in sources:
            sources[src] = []
        sources[src].append(r)

    for src, items in sources.items():
        summary += f"\n\n📦 {src}:"
        for item in items:
            status_icon = {
                "SUCCESS": "✅",
                "UNSUPPORTED": "⏭️",
                "NO_DATA": "❌",
                "EMPTY": "❌",
                "INVALID_DATA": "⚠️",
                "ERROR": "💥",
                "INIT_FAIL": "🔧",
                "IMPORT_FAIL": "📦",
            }.get(item["status"], "❓")

            line = f"\n  {status_icon} {item['sample']:8s} ({item['code']:12s}): {item['status']:15s}"
            if item["rows"]:
                line += f" [{item['rows']} rows]"
            if item.get("date_range"):
                line += f" {item['date_range']}"
            if item["error"]:
                line += f" | {item['error'][:60]}"
            summary += line

    # 统计
    total = len(all_results)
    success = sum(1 for r in all_results if r["status"] == "SUCCESS")
    unsupported = sum(1 for r in all_results if r["status"] == "UNSUPPORTED")
    failed = total - success - unsupported

    summary += f"\n\n📊 总计: {total} 项测试, ✅ {success} 成功, ⏭️ {unsupported} 不支持, ❌ {failed} 失败"
    summary += "\n" + "=" * 80

    logger.info(summary)
    print(summary)

    # 保存CSV结果
    results_df = pd.DataFrame(all_results)
    results_path = os.path.join(str(LOG_DIR), "test_results.csv")
    results_df.to_csv(results_path, index=False, encoding="utf-8-sig")
    logger.info(f"测试结果已保存: {results_path}")


def main():
    logger.info(f"\n{'#'*80}")
    logger.info(f"统一量化数据源系统 - 全面测试")
    logger.info(f"测试时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"测试日期范围: {START_DATE} - {END_DATE}")
    logger.info(f"测试样本: {TEST_SAMPLES}")
    logger.info(f"{'#'*80}")

    all_results = []

    # 1. 测试各数据源
    for source_name, module_path, class_name in SOURCE_CONFIGS:
        results = test_single_source(source_name, module_path, class_name)
        all_results.extend(results)

    # 2. 测试技术指标
    test_indicators()

    # 3. 测试DataManager集成
    test_data_manager()

    # 4. 输出摘要
    print_summary(all_results)


if __name__ == "__main__":
    main()
