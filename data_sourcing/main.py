"""
统一量化数据源系统 - 入口
Usage:
    python main.py <stock_code> <start_date> [end_date]
    python main.py 000001.SZ 20240101
    python main.py 00700.HK 20240101 20241231
"""
import sys
import datetime
from data_manager import DataManager
from config import setup_logger

logger = setup_logger("main", "main.log")


def main():
    if len(sys.argv) < 3:
        print("用法: python main.py <stock_code> <start_date> [end_date]")
        print("示例: python main.py 000001.SZ 20240101")
        print("      python main.py 00700.HK 20240101 20241231")
        sys.exit(1)

    code = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3] if len(sys.argv) > 3 else datetime.date.today().strftime("%Y%m%d")

    logger.info(f"请求: {code}, {start_date} - {end_date}")

    manager = DataManager()
    try:
        df = manager.fetch(code, start_date, end_date)
        if df is not None and not df.empty:
            print(f"\n✅ 获取成功: {code}")
            print(f"   数据量: {len(df)} 条")
            print(f"   日期范围: {df['trade_date'].min()} - {df['trade_date'].max()}")
            print(f"   列: {list(df.columns)}")
            print(f"\n最新5条:")
            print(df.tail().to_string(index=False))
        else:
            print(f"\n❌ 获取失败: {code}")
    finally:
        manager.close_all()


if __name__ == "__main__":
    main()
