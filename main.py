"""
量化交易系统主程序入口
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from quant_system.config_manager import config
from quant_system.logging_config import setup_logging
from quant_system.stock_manager import stock_manager
from quant_system.data_source import unified_data
from quant_system.news_collector import news_collector, news_pipeline
from quant_system.indicators import technical_indicators, indicator_analyzer, update_all_indicators
from quant_system.data_cleaner import data_cleaner, data_validator
from quant_system.feature_extractor import feature_extractor
from quant_system.strategy import strategy_manager, ai_decision_maker
from quant_system.backtest import backtest_engine, backtest_analyzer
from quant_system.risk_manager import risk_manager, risk_report_generator
from quant_system.notification import notification_manager

logger = logging.getLogger(__name__)


def cmd_update_data(args):
    """更新数据命令"""
    logger.info("开始更新数据...")
    
    codes = [args.code] if args.code else None
    
    unified_data.update_all_data(codes=codes, refresh=args.refresh)
    logger.info("数据更新完成")


def cmd_update_indicators(args):
    """更新技术指标命令"""
    logger.info("开始更新技术指标...")
    
    codes = [args.code] if args.code else None
    
    update_all_indicators(codes)
    logger.info("技术指标更新完成")


def cmd_collect_news(args):
    """采集新闻命令"""
    logger.info("开始采集新闻...")
    
    if args.code:
        results = news_pipeline.run(code=args.code)
    else:
        results = news_pipeline.run()
    
    total_news = sum(len(df) for df in results.values())
    logger.info(f"新闻采集完成，共采集 {total_news} 条新闻")


def cmd_extract_features(args):
    """提取特征命令"""
    logger.info("开始提取特征...")
    
    if args.code:
        features = feature_extractor.analyze_with_ai(args.code)
        feature_extractor.save_features(args.code, features)
        logger.info(f"{args.code} 特征提取完成")
    else:
        feature_extractor.extract_all_stocks_features()
        logger.info("所有股票特征提取完成")


def cmd_run_strategy(args):
    """运行策略命令"""
    logger.info(f"运行策略: {args.strategy} on {args.code}")
    
    decision = strategy_manager.run_strategy(args.strategy, args.code)
    
    print("\n" + "="*50)
    print(f"策略决策结果: {args.code}")
    print("="*50)
    print(f"建议操作: {decision.action.upper()}")
    print(f"建议仓位: {decision.position_ratio*100:.1f}%")
    print(f"置信度: {decision.confidence*100:.1f}%")
    print(f"决策理由:\n{decision.reasoning}")
    print("="*50)
    
    # 发送通知
    if args.notify:
        notification_manager.send_strategy_signal(
            args.code, args.strategy,
            decision.action, decision.confidence, decision.reasoning
        )


def cmd_ai_decision(args):
    """AI决策命令"""
    logger.info(f"获取AI决策: {args.code}")
    
    decision = ai_decision_maker.make_decision(args.code, args.description)
    
    print("\n" + "="*50)
    print(f"AI决策结果: {args.code}")
    print("="*50)
    print(f"建议操作: {decision.action.upper()}")
    print(f"建议仓位: {decision.position_ratio*100:.1f}%")
    print(f"置信度: {decision.confidence*100:.1f}%")
    print(f"决策理由:\n{decision.reasoning}")
    print("="*50)


def cmd_backtest(args):
    """回测命令"""
    logger.info(f"开始回测: {args.code} with {args.strategy}")
    
    strategy = strategy_manager.get_strategy(args.strategy)
    if not strategy:
        logger.error(f"策略不存在: {args.strategy}")
        return
    
    result = backtest_engine.run_backtest(
        args.code, strategy,
        args.start_date, args.end_date,
        args.capital
    )
    
    # 打印报告
    report = backtest_analyzer.generate_report(result)
    print(report)
    
    # 发送通知
    if args.notify:
        notification_manager.send_backtest_report(
            args.code, args.strategy,
            {
                'start_date': args.start_date,
                'end_date': args.end_date,
                'total_return_pct': result.total_return_pct,
                'annual_return': result.annual_return,
                'max_drawdown_pct': result.max_drawdown_pct,
                'sharpe_ratio': result.sharpe_ratio,
                'total_trades': result.total_trades,
                'win_rate': result.win_rate,
                'profit_factor': result.profit_factor,
            }
        )


def cmd_risk_report(args):
    """风险报告命令"""
    logger.info("生成风险报告...")
    
    report = risk_report_generator.generate_report(risk_manager)
    print(report)


def cmd_validate_data(args):
    """验证数据命令"""
    logger.info("验证数据...")
    
    results = data_validator.validate_all_data()
    
    print("\n" + "="*50)
    print("数据验证结果")
    print("="*50)
    
    valid_count = 0
    invalid_count = 0
    
    for code, result in results.items():
        status = result.get('status', 'unknown')
        if status == 'valid':
            valid_count += 1
            print(f"✅ {code}: 数据正常")
        elif status == 'invalid':
            invalid_count += 1
            print(f"❌ {code}: 数据异常")
            if 'errors' in result:
                for error in result['errors']:
                    print(f"   - {error}")
        elif status == 'missing':
            print(f"⚠️ {code}: 数据缺失")
        else:
            print(f"❌ {code}: {result.get('error', '未知错误')}")
    
    print("="*50)
    print(f"总计: {valid_count} 正常, {invalid_count} 异常")


def cmd_web(args):
    """启动Web服务"""
    from quant_system.web_app import run_web_server
    
    logger.info("启动Web服务...")
    run_web_server(host=args.host, port=args.port, debug=args.debug)


def cmd_list_stocks(args):
    """列出股票命令"""
    print("\n" + "="*50)
    print("股票列表")
    print("="*50)
    
    stocks = stock_manager.get_all_stocks()
    for stock in stocks:
        type_name = {"stock": "个股", "index": "指数", "sector": "板块"}.get(stock.type, stock.type)
        print(f"{stock.code:10} {stock.name:15} {type_name:6} {stock.market.upper()}")
    
    print("="*50)
    print(f"总计: {len(stocks)} 只股票")


def cmd_list_strategies(args):
    """列出策略命令"""
    print("\n" + "="*50)
    print("策略列表")
    print("="*50)
    
    strategies = strategy_manager.list_strategies()
    for name in strategies:
        strategy = strategy_manager.get_strategy(name)
        print(f"{name:15} {strategy.description[:40] if strategy else ''}")
    
    print("="*50)
    print(f"总计: {len(strategies)} 个策略")


def cmd_indicator_report(args):
    """技术指标报告命令"""
    report = indicator_analyzer.generate_report(args.code)
    print(report)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='量化交易系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py update-data                    # 更新所有数据
  python main.py update-data --code 600519      # 更新指定股票数据
  python main.py run-strategy -c 600519 -s rsi  # 运行策略
  python main.py backtest -c 600519 -s macd     # 回测
  python main.py web                            # 启动Web服务
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 更新数据
    p_update = subparsers.add_parser('update-data', help='更新历史数据')
    p_update.add_argument('--code', help='股票代码')
    p_update.add_argument('--refresh', action='store_true', help='强制刷新')
    p_update.set_defaults(func=cmd_update_data)
    
    # 更新指标
    p_indicators = subparsers.add_parser('update-indicators', help='更新技术指标')
    p_indicators.add_argument('--code', help='股票代码')
    p_indicators.set_defaults(func=cmd_update_indicators)
    
    # 采集新闻
    p_news = subparsers.add_parser('collect-news', help='采集新闻')
    p_news.add_argument('--code', help='股票代码')
    p_news.set_defaults(func=cmd_collect_news)
    
    # 提取特征
    p_features = subparsers.add_parser('extract-features', help='提取特征')
    p_features.add_argument('--code', help='股票代码')
    p_features.set_defaults(func=cmd_extract_features)
    
    # 运行策略
    p_strategy = subparsers.add_parser('run-strategy', help='运行策略')
    p_strategy.add_argument('-c', '--code', required=True, help='股票代码')
    p_strategy.add_argument('-s', '--strategy', required=True, help='策略名称')
    p_strategy.add_argument('-n', '--notify', action='store_true', help='发送通知')
    p_strategy.set_defaults(func=cmd_run_strategy)
    
    # AI决策
    p_ai = subparsers.add_parser('ai-decision', help='AI决策')
    p_ai.add_argument('-c', '--code', required=True, help='股票代码')
    p_ai.add_argument('-d', '--description', help='策略描述')
    p_ai.set_defaults(func=cmd_ai_decision)
    
    # 回测
    p_backtest = subparsers.add_parser('backtest', help='策略回测')
    p_backtest.add_argument('-c', '--code', required=True, help='股票代码')
    p_backtest.add_argument('-s', '--strategy', required=True, help='策略名称')
    p_backtest.add_argument('--start-date', default='20230101', help='开始日期')
    p_backtest.add_argument('--end-date', default=datetime.now().strftime('%Y%m%d'), help='结束日期')
    p_backtest.add_argument('--capital', type=float, default=1000000, help='初始资金')
    p_backtest.add_argument('-n', '--notify', action='store_true', help='发送通知')
    p_backtest.set_defaults(func=cmd_backtest)
    
    # 风险报告
    p_risk = subparsers.add_parser('risk-report', help='风险报告')
    p_risk.set_defaults(func=cmd_risk_report)
    
    # 验证数据
    p_validate = subparsers.add_parser('validate-data', help='验证数据')
    p_validate.set_defaults(func=cmd_validate_data)
    
    # Web服务
    p_web = subparsers.add_parser('web', help='启动Web服务')
    p_web.add_argument('--host', default='0.0.0.0', help='主机地址')
    p_web.add_argument('--port', type=int, default=8080, help='端口')
    p_web.add_argument('--debug', action='store_true', help='调试模式')
    p_web.set_defaults(func=cmd_web)
    
    # 列出股票
    p_list = subparsers.add_parser('list-stocks', help='列出股票')
    p_list.set_defaults(func=cmd_list_stocks)
    
    # 列出策略
    p_strategies = subparsers.add_parser('list-strategies', help='列出策略')
    p_strategies.set_defaults(func=cmd_list_strategies)
    
    # 技术指标报告
    p_report = subparsers.add_parser('indicator-report', help='技术指标报告')
    p_report.add_argument('-c', '--code', required=True, help='股票代码')
    p_report.set_defaults(func=cmd_indicator_report)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 设置日志
    setup_logging(config)
    # 启动时更新交易日（A股 / 港股）并保存到 data/trading_dates.json
    try:
        from data_sourcing.trading_calendar import update_trading_dates
        update_trading_dates()
    except Exception as e:
        logger.warning(f"更新交易日失败: {e}")

    # 执行命令
    args.func(args)


if __name__ == '__main__':
    main()
