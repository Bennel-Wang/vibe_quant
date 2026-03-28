"""
CSV文件迁移脚本
将历史数据文件从 {code}_daily.csv 格式重命名为 {market}{code}_daily.csv 格式
例如: 600519_daily.csv -> sh600519_daily.csv
"""

import os
import sys
import shutil

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quant_system.stock_manager import stock_manager
from quant_system.config_manager import config


def migrate_csv_names(dry_run=True):
    """
    迁移CSV文件名，添加市场前缀

    Args:
        dry_run: 如果为True，只显示将要执行的操作而不实际执行
    """
    data_dirs = config.get_data_dirs()
    history_dir = data_dirs.get('history', './data/history')
    indicators_dir = data_dirs.get('indicators', './data/indicators')

    stocks = stock_manager.get_all_stocks()

    print("=" * 60)
    print("CSV文件名迁移工具")
    print(f"{'[预览模式]' if dry_run else '[执行模式]'}")
    print("=" * 60)

    rename_count = 0
    skip_count = 0

    for stock in stocks:
        old_code = stock.code
        new_code = stock.storage_code

        if old_code == new_code:
            print(f"  跳过 {stock.name}({old_code}) - 无需重命名")
            skip_count += 1
            continue

        # 迁移历史数据文件
        old_path = os.path.join(history_dir, f"{old_code}_daily.csv")
        new_path = os.path.join(history_dir, f"{new_code}_daily.csv")

        if os.path.exists(old_path) and not os.path.exists(new_path):
            print(f"  重命名: {old_code}_daily.csv -> {new_code}_daily.csv")
            if not dry_run:
                shutil.move(old_path, new_path)
            rename_count += 1
        elif os.path.exists(new_path):
            print(f"  跳过 {new_code}_daily.csv - 新文件已存在")
            skip_count += 1
        elif not os.path.exists(old_path):
            print(f"  跳过 {old_code}_daily.csv - 旧文件不存在")
            skip_count += 1

        # 迁移指标文件
        for freq in ['day', 'week', 'month']:
            old_ind = os.path.join(indicators_dir, f"{old_code}_indicators_{freq}.csv")
            new_ind = os.path.join(indicators_dir, f"{new_code}_indicators_{freq}.csv")

            if os.path.exists(old_ind) and not os.path.exists(new_ind):
                print(f"  重命名: {old_code}_indicators_{freq}.csv -> {new_code}_indicators_{freq}.csv")
                if not dry_run:
                    shutil.move(old_ind, new_ind)
                rename_count += 1

    print("\n" + "=" * 60)
    print(f"总计: {rename_count} 个文件{'将被' if dry_run else '已'}重命名, {skip_count} 个跳过")
    if dry_run:
        print("\n提示: 使用 --execute 参数执行实际重命名操作")
    print("=" * 60)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='迁移CSV文件名（添加市场前缀）')
    parser.add_argument('--execute', action='store_true',
                        help='执行实际重命名操作（默认仅预览）')
    args = parser.parse_args()

    migrate_csv_names(dry_run=not args.execute)
