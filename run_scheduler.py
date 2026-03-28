"""
定时任务运行脚本
用于启动定时调度器

使用方法:
    python run_scheduler.py           # 启动调度器
    python run_scheduler.py --once    # 立即运行一次
    python run_scheduler.py --stop    # 停止调度器（需要配合PID文件使用）
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from quant_system.scheduler import scheduler, start_scheduler
from quant_system.config_manager import config

# 配置日志
def setup_logging():
    """设置日志"""
    log_dir = Path('./logs')
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('./logs/scheduler.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def main():
    parser = argparse.ArgumentParser(description='量化交易系统定时任务调度器')
    parser.add_argument('--once', action='store_true', help='立即运行一次任务')
    parser.add_argument('--force', action='store_true', help='强制执行，跳过交易日检查（用于测试）')
    parser.add_argument('--status', action='store_true', help='查看调度器状态')
    parser.add_argument('--config', action='store_true', help='配置调度器')
    
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    if args.once:
        # 立即运行一次
        logger.info("手动触发任务执行...")
        scheduler.run_once(force=args.force)
    
    elif args.status:
        # 查看状态
        status = scheduler.get_status()
        print("\n" + "=" * 60)
        print("调度器状态")
        print("=" * 60)
        print(f"运行状态: {'运行中' if status['is_running'] else '已停止'}")
        print(f"是否交易日: {'是' if status['is_trading_day'] else '否'}")
        print(f"启用状态: {'已启用' if status['config'].get('enabled') else '已禁用'}")
        print(f"\n定时时间: {', '.join(status['config'].get('afternoon_times', []))}")
        print(f"\n选中股票: {', '.join(status['config'].get('selected_stocks', [])) or '全部'}")
        print(f"\n任务配置:")
        for task, enabled in status['config'].get('tasks', {}).items():
            print(f"  - {task}: {'启用' if enabled else '禁用'}")
        if status['next_run_times']:
            print(f"\n下次运行时间:")
            for t in status['next_run_times']:
                print(f"  - {t}")
        print("=" * 60)
    
    elif args.config:
        # 交互式配置
        print("\n" + "=" * 60)
        print("调度器配置")
        print("=" * 60)
        
        # 启用/禁用
        enabled = input("是否启用调度器? (y/n): ").lower() == 'y'
        
        # 定时时间
        times_input = input("请输入定时时间 (多个时间用逗号分隔, 如 14:25,14:45): ")
        afternoon_times = [t.strip() for t in times_input.split(',') if t.strip()]
        
        # 选择股票
        from quant_system.stock_manager import stock_manager
        stocks = stock_manager.get_all_stocks()
        print("\n可用股票:")
        for i, s in enumerate(stocks, 1):
            print(f"{i}. {s.name} ({s.code})")
        
        stocks_input = input("\n请选择要分析的股票编号 (多个用逗号分隔, 留空表示全部): ")
        if stocks_input.strip():
            selected_indices = [int(x.strip()) - 1 for x in stocks_input.split(',')]
            selected_stocks = [stocks[i].code for i in selected_indices if 0 <= i < len(stocks)]
        else:
            selected_stocks = []
        
        # 任务配置
        print("\n任务配置:")
        tasks = {}
        tasks['update_data'] = input("更新数据? (y/n): ").lower() != 'n'
        tasks['update_news'] = input("更新新闻? (y/n): ").lower() != 'n'
        tasks['update_indicators'] = input("更新技术指标? (y/n): ").lower() != 'n'
        tasks['ai_analysis'] = input("AI分析? (y/n): ").lower() != 'n'
        tasks['send_notification'] = input("发送通知? (y/n): ").lower() != 'n'
        
        # 更新配置
        new_config = {
            'enabled': enabled,
            'afternoon_times': afternoon_times,
            'selected_stocks': selected_stocks,
            'tasks': tasks
        }
        
        scheduler.update_config(new_config)
        print("\n配置已保存!")
        print("=" * 60)
    
    else:
        # 启动调度器
        print("\n" + "=" * 60)
        print("启动量化交易系统定时任务调度器")
        print("=" * 60)
        print("按 Ctrl+C 停止")
        print("=" * 60 + "\n")
        
        start_scheduler()


if __name__ == '__main__':
    main()
