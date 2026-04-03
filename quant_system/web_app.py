"""
Web可视化界面
使用Flask提供Web服务，展示数据、策略、回测结果
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from flask import Flask, render_template, jsonify, request, send_from_directory
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly.utils import PlotlyJSONEncoder

from .config_manager import config
from .stock_manager import stock_manager
from .data_source import unified_data
from .indicators import indicator_analyzer, technical_indicators, fresh_technical_indicators
from .strategy import strategy_manager, ai_decision_maker
from .backtest import backtest_engine, backtest_analyzer
from .risk_manager import risk_manager, risk_report_generator, Position
from .feature_extractor import feature_extractor
from .news_collector import news_collector, sentiment_analyzer
from .scheduler import scheduler
from .notification import notification_manager, PushPlusNotifier

# 数据持久化路径
DATA_STATE_PATH = os.path.join(config.get('data_storage.data_dir', './data'), 'system_state.json')

logger = logging.getLogger(__name__)

# 后台回测进度与结果存储（内存缓存，必要时可替换为Redis等持久化）
BACKTEST_PROGRESS: Dict[str, Dict] = {}
BACKTEST_RESULTS: Dict[str, Dict] = {}

# 策略适配（遍历股票）进度与结果存储
ADAPT_PROGRESS: Dict[str, Dict] = {}
ADAPT_RESULTS: Dict[str, Dict] = {}

# Intraday snapshot storage: code -> list of {time, price, volume, avg_price}
INTRADAY_SNAPSHOTS: Dict[str, list] = {}


def is_trading_time(market: str = None) -> bool:
    """判断当前是否在交易时间内（北京时间）。
    market=None 表示任一市场在交易即返回 True。
    A股: 周一至周五 09:30-11:30 / 13:00-15:00
    港股: 周一至周五 09:30-12:00 / 13:00-16:00
    """
    now = datetime.now()
    if now.weekday() >= 5:  # 周六/周日
        return False
    minutes = now.hour * 60 + now.minute

    def _a_share():
        return (9 * 60 + 30 <= minutes <= 11 * 60 + 30) or (13 * 60 <= minutes <= 15 * 60)

    def _hk():
        return (9 * 60 + 30 <= minutes <= 12 * 60) or (13 * 60 <= minutes <= 16 * 60)

    if market == 'hk':
        return _hk()
    if market in ('sh', 'sz'):
        return _a_share()
    # market=None：任一市场有交易即返回 True
    return _a_share() or _hk()


# 创建Flask应用
app = Flask(__name__, 
            template_folder='templates',
            static_folder='static')


# ============== 辅助函数 ==============

def resample_to_weekly(df):
    """将日线数据重采样为周线"""
    if df.empty:
        return df
    
    df = df.copy()
    df.set_index('date', inplace=True)
    
    # 按周重采样
    weekly = df.resample('W').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    })
    
    weekly = weekly.dropna()
    weekly = weekly.reset_index()
    
    return weekly


def resample_to_monthly(df):
    """将日线数据重采样为月线"""
    if df.empty:
        return df
    
    df = df.copy()
    df.set_index('date', inplace=True)
    
    # 按月重采样（兼容不同pandas版本）
    try:
        monthly = df.resample('M').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
    except ValueError:
        # 如果'M'不行，尝试'MS'
        monthly = df.resample('MS').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
    
    monthly = monthly.dropna()
    monthly = monthly.reset_index()
    
    return monthly


# Yearly K-line functionality removed; only day/week/month frequencies are supported.
# If yearly resampling is required in future, re-implement here.


# ============== 路由定义 ==============

@app.route('/')
def index():
    """首页"""
    return render_template('index.html')


@app.route('/api/stocks')
def api_stocks():
    """获取股票列表"""
    stocks = stock_manager.get_all_stocks()

    def _safe(v):
        """将 NaN/None 等非 JSON 值转为空字符串"""
        if v is None:
            return ''
        if isinstance(v, float) and v != v:  # NaN check
            return ''
        return v

    data = [{
        'code': s.code,
        'name': s.name,
        'market': s.market,
        'type': s.type,
        'full_code': s.full_code,
        'notes': _safe(s.notes),
        'industry': _safe(s.industry),
        'strategy': _safe(s.strategy),
        'buy_strategy': _safe(getattr(s, 'buy_strategy', '')),
        'sell_strategy': _safe(getattr(s, 'sell_strategy', '')),
        'category': 'index' if s.type == 'index' else ('sector' if s.type == 'sector' else 'stock'),
    } for s in stocks]
    return jsonify(data)


@app.route('/api/stocks/add', methods=['POST'])
def api_stocks_add():
    """添加股票到监控列表（支持代码或名称，自动验证）"""
    try:
        data = request.json or {}
        code = data.get('code', '').strip()
        name = data.get('name', '').strip()
        market = data.get('market', '').strip()
        stock_type = data.get('type', 'stock').strip()
        industry = ''

        if not code and not name:
            return jsonify({'error': '请提供股票代码或名称'}), 400

        # 加载本地股票列表（一次性）
        stock_list_path = os.path.join(config.get('data_storage.data_dir', './data'), 'stock_list.csv')
        df_list = None
        if os.path.exists(stock_list_path):
            try:
                df_list = pd.read_csv(stock_list_path, encoding='utf-8', dtype=str)
            except Exception as e:
                logger.warning(f"加载股票列表失败: {e}")

        # 如果只提供了名称，尝试通过本地股票列表查找代码
        if not code and name:
            if df_list is not None:
                matches = df_list[df_list['name'] == name]
                if matches.empty:
                    matches = df_list[df_list['name'].str.contains(name, case=False, na=False)]
                if not matches.empty:
                    row = matches.iloc[0]
                    ts_code = row['ts_code']
                    code = ts_code.split('.')[0]
                    if not market:
                        suffix = ts_code.split('.')[1].upper() if '.' in ts_code else ''
                        market = {'SH': 'sh', 'SZ': 'sz', 'HK': 'hk'}.get(suffix, 'sh')
                    name = row['name']
                    _ind = row.get('industry', '')
                    industry = '' if pd.isna(_ind) else (str(_ind).strip() or '')
                else:
                    return jsonify({'error': f'未找到名为 "{name}" 的股票'}), 400
            else:
                return jsonify({'error': '股票列表文件不存在，无法通过名称查找'}), 400

        if not code:
            return jsonify({'error': '请提供股票代码'}), 400

        # 要求用户明确选择市场，不再自动推断
        if not market:
            return jsonify({'error': '请选择市场（上海/深圳/港股）'}), 400

        # 检查是否已存在（需考虑相同代码不同市场的情况，如 000001.SH 上证指数 vs 000001.SZ 平安银行）
        for s in stock_manager.get_all_stocks():
            if s.code == code and s.market == market:
                return jsonify({'error': f'股票已存在: {s.name}({s.code})'}), 400

        # 验证股票代码并获取名称/板块（通过本地股票列表查询）
        validated = False
        if df_list is not None and (not name or name == code or not industry):
            suffix = {'sh': 'SH', 'sz': 'SZ', 'hk': 'HK'}.get(market, 'SH')
            ts_code = f'{code}.{suffix}'
            match = df_list[df_list['ts_code'] == ts_code]
            if match.empty:
                # 仅在未明确指定市场时尝试备用后缀，且不覆盖已推断的 market
                # 避免 000001.SZ(平安银行) 误匹配到 000001.SH(上证指数)
                alt_suffix = 'SZ' if suffix == 'SH' else 'SH'
                alt_match = df_list[df_list['ts_code'] == f'{code}.{alt_suffix}']
                if not alt_match.empty:
                    # 只有当用户未通过前端明确指定市场时，才允许切换
                    # 注意：此处不修改 market，保留推断结果以避免跨市场混淆
                    match = alt_match
                    # DO NOT override market here — keeps sz/sh distinction correct
            if not match.empty:
                row = match.iloc[0]
                if not name or name == code:
                    name = row['name']
                if not industry:
                    _ind = row.get('industry', '')
                    industry = '' if pd.isna(_ind) else (str(_ind).strip() or '')
                validated = True

        if name and name != code:
            validated = True

        if not validated:
            return jsonify({'error': f'无效的股票代码: {code}，请检查代码是否正确'}), 400

        stock_manager.add_stock(name=name, code=code, market=market,
                                stock_type=stock_type, industry=industry, save=True)

        return jsonify({
            'success': True,
            'message': f'已添加 {name}({code})',
            'stock': {'code': code, 'name': name, 'market': market, 'type': stock_type, 'industry': industry}
        })
    except Exception as e:
        logger.error(f"添加股票失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stocks/<code>', methods=['DELETE'])
def api_stocks_delete(code):
    """从监控列表移除股票"""
    try:
        existing = stock_manager.get_stock_by_code(code)
        if not existing:
            return jsonify({'error': f'股票不存在: {code}'}), 404

        stock_manager.remove_stock(code, save=True)
        # 从实时快照中清除，防止前端轮询时重建已删除股票的行
        for key in [existing.full_code, existing.code, code]:
            realtime_snapshot.pop(key, None)
        return jsonify({
            'success': True,
            'message': f'已移除 {existing.name}({existing.code})'
        })
    except Exception as e:
        logger.error(f"移除股票失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stocks/<code>/notes', methods=['PUT'])
def api_stocks_update_notes(code):
    """更新股票备注"""
    try:
        stock = stock_manager.get_stock_by_code(code)
        if not stock:
            return jsonify({'error': f'股票不存在: {code}'}), 404
        data = request.json or {}
        stock.notes = data.get('notes', '')
        stock_manager.save()
        return jsonify({'success': True, 'message': '备注已更新', 'notes': stock.notes})
    except Exception as e:
        logger.error(f"更新备注失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stocks/search')
def api_stocks_search():
    """搜索/验证股票代码（基于本地股票列表文件，支持代码或名称模糊搜索）"""
    query = request.args.get('q', '').strip()
    if not query or len(query) < 1:
        return jsonify([])

    results = []
    existing_codes = set()

    # 1. 从已有的股票列表中搜索
    for stock in stock_manager.get_all_stocks():
        if (query in stock.code or query.upper() in stock.name.upper()
                or query in stock.full_code):
            results.append({
                'code': stock.code, 'name': stock.name,
                'market': stock.market, 'type': stock.type,
                'exists': True
            })
            existing_codes.add(stock.code)

    # 2. 从本地 stock_list.csv 搜索
    if len(query) >= 2:
        try:
            if not hasattr(api_stocks_search, '_cache') or api_stocks_search._cache is None:
                stock_list_path = os.path.join(config.get('data_storage.data_dir', './data'), 'stock_list.csv')
                if os.path.exists(stock_list_path):
                    api_stocks_search._cache = pd.read_csv(stock_list_path, encoding='utf-8', dtype=str)
                    logger.info(f"已加载股票列表: {len(api_stocks_search._cache)} 条")
                else:
                    api_stocks_search._cache = pd.DataFrame()
                    logger.warning(f"股票列表文件不存在: {stock_list_path}")
            df = api_stocks_search._cache
            if df is not None and not df.empty:
                # 同时支持代码和名称搜索
                query_upper = query.upper()
                mask_code = df['ts_code'].str.contains(query, case=False, na=False)
                mask_name = df['name'].str.contains(query, case=False, na=False)
                matches = df[mask_code | mask_name]
                for _, row in matches.head(15).iterrows():
                    ts_code = str(row.get('ts_code', ''))
                    bare_code = ts_code.split('.')[0]
                    if bare_code in existing_codes:
                        continue
                    if '.' in ts_code:
                        suffix = ts_code.split('.')[1].upper()
                        if suffix == 'SH':
                            market_val = 'sh'
                        elif suffix == 'SZ':
                            market_val = 'sz'
                        elif suffix == 'HK':
                            market_val = 'hk'
                        else:
                            market_val = str(row.get('market', 'sh')).lower()
                    else:
                        market_val = str(row.get('market', 'sh')).lower()
                    results.append({
                        'code': bare_code, 'name': str(row.get('name', '')),
                        'market': market_val, 'type': 'stock',
                        'exists': False
                    })
                    existing_codes.add(bare_code)
        except Exception as e:
            logger.debug(f"本地股票列表搜索失败: {e}")

    return jsonify(results[:15])


@app.route('/api/stock/<code>/data')
def api_stock_data(code):
    """获取股票历史数据"""
    try:
        start_date = request.args.get('start', 
            (datetime.now() - timedelta(days=365)).strftime('%Y%m%d'))
        end_date = request.args.get('end', datetime.now().strftime('%Y%m%d'))
        freq = request.args.get('freq', 'day')
        
        df = unified_data.get_historical_data(code, start_date, end_date, freq)
        
        if df.empty:
            return jsonify({'error': 'No data available'}), 404
        
        # 转换为JSON
        data = df.to_dict(orient='records')
        return jsonify(data)
        
    except Exception as e:
        logger.error(f"获取股票数据失败 {code}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stock/<code>/indicators')
def api_stock_indicators(code):
    """获取股票技术指标"""
    try:
        freq = request.args.get('freq', 'day')
        
        # 获取股票信息
        stock = stock_manager.get_stock_by_code(code)
        if not stock:
            return jsonify({'error': 'Stock not found'}), 404
        
        # 使用 full_code（000001.SH 格式）查找指标文件
        stock_code_for_indicator = stock.full_code
        
        # 尝试加载已计算的指标
        df = technical_indicators.load_indicators(stock_code_for_indicator, freq)
        
        # RSI需要至少 period+1 行数据，若行数太少则重新计算
        MIN_ROWS_FOR_INDICATORS = 30
        if df.empty or len(df) < MIN_ROWS_FOR_INDICATORS:
            if not df.empty:
                logger.info(f"{stock_code_for_indicator} 指标数据仅 {len(df)} 行，不足以计算RSI/MACD，重新计算...")
            else:
                logger.info(f"未找到 {stock_code_for_indicator} 的指标数据，尝试重新计算...")
            try:
                df = technical_indicators.calculate_all_indicators(stock_code_for_indicator, freq=freq)
                if not df.empty:
                    technical_indicators.save_indicators(stock_code_for_indicator, df, freq)
            except Exception as e:
                logger.warning(f"计算指标失败: {e}")
        
        if df.empty:
            return jsonify({'error': 'No indicator data available'}), 404
        
        # 确保数据格式正确
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        # pe_ttm/pb/pettm_pct10y 仅在每个交易日收盘后由 tushare daily_basic 更新，
        # 最新行可能因实时OHLCV数据晚于基本面更新而为 NaN。前向填充以使用最近有效值。
        for _ffill_col in ('pe_ttm', 'pb', 'pettm_pct10y'):
            if _ffill_col in df.columns:
                df[_ffill_col] = df[_ffill_col].ffill()

        # 获取最新数据
        latest = df.iloc[-1].to_dict()

        # 若指标文件中没有预计算百分位，则在此实时计算
        if 'rsi6_pct100' not in latest or latest.get('rsi6_pct100') is None:
            try:
                import numpy as _np2
                rsi_col = df['rsi_6'].values.astype(float) if 'rsi_6' in df.columns else None
                if rsi_col is not None and len(rsi_col) > 1:
                    window_start = max(0, len(rsi_col) - 101)
                    past = rsi_col[window_start:-1]
                    valid = past[~_np2.isnan(past)]
                    cur = rsi_col[-1]
                    if len(valid) > 0 and not _np2.isnan(cur):
                        latest['rsi6_pct100'] = round(float((valid <= cur).sum()) / len(valid) * 100, 1)
            except Exception:
                pass
        if 'pettm_pct10y' not in latest or latest.get('pettm_pct10y') is None:
            try:
                import numpy as _np3
                if 'pe_ttm' in df.columns:
                    pe_col = df['pe_ttm'].values.astype(float)
                    # Use last non-NaN positive pe_ttm value (ffill may not cover all cases)
                    valid_pe_all = pe_col[(~_np3.isnan(pe_col)) & (pe_col > 0)]
                    cur_pe = valid_pe_all[-1] if len(valid_pe_all) > 0 else float('nan')
                    if not _np3.isnan(cur_pe) and cur_pe > 0:
                        past_valid = valid_pe_all[:-1]
                        if len(past_valid) > 0:
                            latest['pettm_pct10y'] = round(float((past_valid <= cur_pe).sum()) / len(past_valid) * 100, 1)
            except Exception:
                pass
        
        # 处理NaN值和numpy类型
        import math as _math
        import numpy as _np
        for key, value in list(latest.items()):
            try:
                if isinstance(value, _np.integer):
                    latest[key] = int(value)
                elif isinstance(value, (_np.floating, float)):
                    fv = float(value)
                    if _math.isnan(fv) or _math.isinf(fv):
                        latest[key] = None
                    else:
                        if key.startswith('rsi'):
                            latest[key] = round(fv, 2)
                        elif key.startswith('macd'):
                            latest[key] = round(fv, 4)
                        elif key.startswith(('ma_', 'kdj')):
                            latest[key] = round(fv, 2)
                        elif key in ('pe_ttm', 'pb'):
                            latest[key] = round(fv, 2)
                        else:
                            latest[key] = fv
                elif pd.isna(value):
                    latest[key] = None
            except (TypeError, ValueError):
                pass
        
        # 日期序列化
        if isinstance(latest.get('date'), pd.Timestamp):
            latest['date'] = latest['date'].strftime('%Y-%m-%d')
        
        # 返回最新数据和历史数据
        history = df.tail(30).copy()
        history['date'] = history['date'].dt.strftime('%Y-%m-%d')
        # 将所有 NaN/inf 替换为 None，避免 JSON 中出现非法的 NaN 字面量
        import numpy as _np_hist
        history = history.replace([_np_hist.nan, _np_hist.inf, -_np_hist.inf], None)
        return jsonify({
            'latest': latest,
            'history': history.to_dict(orient='records')
        })
        
    except Exception as e:
        logger.error(f"获取技术指标失败 {code}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stock/<code>/chart')
def api_stock_chart(code):
    """获取股票K线图数据"""
    try:
        # 获取周期参数
        freq = request.args.get('freq', 'day')  # day, week, month
        
        # 获取 n 参数（用于限制显示最近 n 个周期，默认为 None 即不限制）
        try:
            n_param = request.args.get('n')
            n_param = int(n_param) if n_param is not None else None
        except Exception:
            n_param = None
        
        # 根据周期设置默认时间范围
        if freq == 'day':
            default_days = 180
        elif freq == 'week':
            default_days = 365
        elif freq == 'month':
            default_days = 365 * 2
        else:
            default_days = 180
        
        # 保存原始周期用于重采样
        original_freq = freq
        
        # 如果请求中没有指定 start，则按周期使用有限的图表窗口（避免加载22年全量数据）
        # cfg_start 仅用于数据采集，不用于图表默认范围
        if request.args.get('start'):
            start_date = request.args.get('start')
        else:
            # 图表显示窗口：日线2年，周线5年，月线10年
            if freq == 'day':
                chart_days = 730
            elif freq == 'week':
                chart_days = 365 * 5
            else:
                chart_days = 365 * 10
            start_date = (datetime.now() - timedelta(days=chart_days)).strftime('%Y%m%d')

        end_date = request.args.get('end', datetime.now().strftime('%Y%m%d'))
        
        # 获取股票信息
        stock = stock_manager.get_stock_by_code(code)
        stock_name = stock.name if stock else code
        
        # 板块数据现在支持K线图展示
        # if stock and stock.type == 'sector':
        #     return jsonify({'error': '板块暂不支持K线图展示'}), 404
        
        # 获取对应周期的历史数据（默认拉取日线，再根据 freq 重采样），并请求前复权数据
        df = unified_data.get_historical_data(code, start_date, end_date, freq='day', adjust=True)
        
        logger.info(f"获取到 {code} 的历史数据，共 {len(df)} 条")
        
        if df.empty:
            logger.warning(f"未找到 {code} 的历史数据，尝试刷新获取")
            # 尝试重新获取数据
            df = unified_data.get_historical_data(code, start_date, end_date, freq='day', adjust=True)
            
            if df.empty:
                return jsonify({'error': 'No data available'}), 404
            
            # 标准化列名
            df = unified_data._standardize_columns(df)
        
        # 确保日期格式正确
        # date 列可能是字符串 (20260312) 或 datetime，统一转换为 datetime
        if df['date'].dtype == 'object':
            # 字符串格式，转换为 datetime
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d', errors='coerce')
        else:
            # 已经是 datetime，确保格式正确
            df['date'] = pd.to_datetime(df['date'])
        
        df = df.dropna(subset=['date'])
        df = df.sort_values('date')
        
        logger.info(f"日期处理后剩余 {len(df)} 条数据，日期范围: {df['date'].min()} 到 {df['date'].max()}")
        
        # 打印前3条数据用于调试
        if len(df) > 0:
            sample = df.head(3)[['date', 'open', 'high', 'low', 'close']].to_dict('records')
            logger.info(f"数据样例: {sample}")
        
        # 使用配置中的历史起始日来获取完整数据并在显示时按窗口 n 截取
        # df 当前为从 unified_data 获取的完整日线数据（前复权）
        df_full = df.copy()

        # 对周期进行重采样，但保留完整数据用于计算/保存指标
        if original_freq == 'week':
            resampled_full = resample_to_weekly(df_full)
        elif original_freq == 'month':
            resampled_full = resample_to_monthly(df_full)
        else:
            resampled_full = df_full.copy()

        # 保证日期格式
        resampled_full['date'] = pd.to_datetime(resampled_full['date'])
        resampled_full = resampled_full.sort_values('date').reset_index(drop=True)

        # 为显示创建 df（按 n_param 截取）——仅影响前端显示，不改变存储的数据
        try:
            if n_param is not None and n_param > 0:
                df = resampled_full.tail(n_param).reset_index(drop=True)
            else:
                df = resampled_full.copy()
        except Exception as e:
            logger.warning(f'限制显示数据数量时发生错误: {e}')
            df = resampled_full.copy()

        # 确保价格数据是数值类型（仅对显示数据）
        for col in ['open', 'high', 'low', 'close']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 移除无效数据（显示数据）
        df = df.dropna(subset=['open', 'high', 'low', 'close'])
        if df.empty:
            return jsonify({'error': 'No valid price data available for display'}), 404

        logger.info(f"显示价格数据范围: open {df['open'].min():.2f}-{df['open'].max():.2f}, close {df['close'].min():.2f}-{df['close'].max():.2f}")

        # 指标数据的生成/加载应基于完整的重采样数据（resampled_full）——这样CSV保存与计算不会因为显示窗口而被截断
        df_ind = pd.DataFrame()
        if original_freq in ['week', 'month']:
            try:
                logger.info('为周/月线强制重新计算指标（使用完整重采样结果）')
                df_for_indicators = resampled_full.copy()
                df_for_indicators['date'] = pd.to_datetime(df_for_indicators['date'])
                df_ind = technical_indicators.calculate_all_indicators_from_df(df_for_indicators)
                if not df_ind.empty:
                    try:
                        technical_indicators.save_indicators(code, df_ind, freq=original_freq)
                    except Exception as e:
                        logger.warning(f'保存周/月线指标CSV失败: {e}')
            except Exception as e:
                logger.error(f'周/月线指标重新计算失败: {e}')
                df_ind = pd.DataFrame()
        else:
            # 日线优先从CSV加载（完整数据），不存在则计算并保存（基于完整日线 df_full）
            try:
                logger.info('尝试从CSV加载日线指标数据（完整）')
                df_ind = technical_indicators.load_indicators(code, freq=original_freq)
                if not df_ind.empty:
                    df_ind['date'] = pd.to_datetime(df_ind['date'])
                    df_ind = df_ind.sort_values('date')
                    # If loaded CSV is missing newer indicator columns, recalculate
                    if 'kdj_k' not in df_ind.columns or 'boll_upper' not in df_ind.columns:
                        logger.info('CSV缺少KDJ/BOLL指标，重新计算并保存')
                        try:
                            df_for_indicators = df_full.copy()
                            df_for_indicators['date'] = pd.to_datetime(df_for_indicators['date'])
                            df_ind = technical_indicators.calculate_all_indicators_from_df(df_for_indicators)
                            if not df_ind.empty:
                                df_ind['date'] = pd.to_datetime(df_ind['date'])
                                df_ind = df_ind.sort_values('date')
                                try:
                                    technical_indicators.save_indicators(code, df_ind, freq=original_freq)
                                except Exception:
                                    pass
                        except Exception as e:
                            logger.warning(f'重新计算指标失败: {e}')
                    logger.info(f'已从CSV加载指标: {code}, freq={original_freq}, shape={df_ind.shape}')
                else:
                    logger.info('未找到CSV指标文件（日线），使用计算方法生成（完整）')
                    try:
                        df_ind = fresh_technical_indicators.calculate_fresh_indicators(code, start_date, end_date)
                        if not df_ind.empty:
                            df_ind['date'] = pd.to_datetime(df_ind['date'])
                            df_ind = df_ind.sort_values('date')
                            try:
                                technical_indicators.save_indicators(code, df_ind, freq=original_freq)
                            except Exception as e:
                                logger.warning(f'保存指标CSV失败: {e}')
                    except Exception as e:
                        logger.warning(f'全新方法计算失败: {e}, 尝试回退到原有计算方法（完整）')
                        try:
                            df_for_indicators = df_full.copy()
                            df_for_indicators['date'] = pd.to_datetime(df_for_indicators['date'])
                            df_ind = technical_indicators.calculate_all_indicators_from_df(df_for_indicators)
                            if not df_ind.empty:
                                technical_indicators.save_indicators(code, df_ind, freq=original_freq)
                        except Exception as e2:
                            logger.error(f'回退计算也失败: {e2}')
            except Exception as e:
                logger.error(f'加载或计算指标时发生错误: {e}')
                try:
                    df_for_indicators = df_full.copy()
                    df_for_indicators['date'] = pd.to_datetime(df_for_indicators['date'])
                    df_ind = technical_indicators.calculate_all_indicators_from_df(df_for_indicators)
                    if not df_ind.empty:
                        technical_indicators.save_indicators(code, df_ind, freq=original_freq)
                except Exception as e2:
                    logger.error(f'回退计算也失败: {e2}')
        
        # 创建4行子图：价格+均线 / 成交量 / MACD / RSI
        fig = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            row_heights=[0.5, 0.12, 0.18, 0.20],
            vertical_spacing=0.03,
            subplot_titles=('', '成交量', 'MACD', 'RSI')
        )
        
        # 打印K线数据用于调试
        logger.info(f"K线数据: 日期数={len(df)}, open范围={df['open'].min():.2f}-{df['open'].max():.2f}, close范围={df['close'].min():.2f}-{df['close'].max():.2f}")
        
        # 检查数据完整性
        if len(df) < 10:
            logger.warning(f"数据点过少 ({len(df)} 个)，可能存在问题")
            if len(df) > 0:
                logger.info(f"前几条数据: {df[['date', 'open', 'close']].head().to_dict('records')}")
        
        # 添加K线
        # 使用日期字符串作为 x（设置为 category 类型以避免交易日间隙），这样鼠标悬停显示日期
        dates_str = df['date'].dt.strftime('%Y-%m-%d').tolist()
        open_vals  = df['open'].tolist()
        high_vals  = df['high'].tolist()
        low_vals   = df['low'].tolist()
        close_vals = df['close'].tolist()

        # 计算每日涨跌幅（相对前收）
        pct_texts = []
        prev_close = [None] + close_vals[:-1]
        for c, p in zip(close_vals, prev_close):
            if p is not None and p != 0:
                pct = (c - p) / p * 100
                sign = '+' if pct >= 0 else ''
                pct_texts.append(f'{sign}{pct:.2f}%')
            else:
                pct_texts.append('--')

        # K线主图（关闭内置 hover，由下方 Scatter 接管以确保 customdata 显示正常）
        fig.add_trace(go.Candlestick(
            x=dates_str,
            open=open_vals,
            high=high_vals,
            low=low_vals,
            close=close_vals,
            name='K线',
            increasing_line_color='#ef5350',
            increasing_fillcolor='#ef5350',
            decreasing_line_color='#26a69a',
            decreasing_fillcolor='#26a69a',
            hoverinfo='none',       # 由 Scatter overlay 统一处理 hover
            showlegend=True,
        ), row=1, col=1)

        # 透明 Scatter overlay：承载完整 hover（含涨跌幅）
        # Plotly Candlestick 的 customdata/hovertemplate 支持不稳定，用 Scatter 是最可靠方案
        mid_vals = [(o + c) / 2 for o, c in zip(open_vals, close_vals)]
        hover_customdata = [
            [o, h, l, c, pct]
            for o, h, l, c, pct in zip(open_vals, high_vals, low_vals, close_vals, pct_texts)
        ]
        fig.add_trace(go.Scatter(
            x=dates_str,
            y=mid_vals,
            mode='markers',
            marker=dict(size=14, opacity=0, color='rgba(0,0,0,0)'),
            customdata=hover_customdata,
            hovertemplate=(
                '<b>%{x}</b><br>'
                '开: %{customdata[0]:.2f}<br>'
                '高: %{customdata[1]:.2f}<br>'
                '低: %{customdata[2]:.2f}<br>'
                '收: %{customdata[3]:.2f}<br>'
                '涨跌幅: <b>%{customdata[4]}</b>'
                '<extra></extra>'
            ),
            name='',
            showlegend=False,
        ), row=1, col=1)
        
        # 添加均线（只要有均线数据即绘制，支持日/周/月）
        if not df_ind.empty and any(col in df_ind.columns for col in ['ma_5', 'ma_20', 'ma_60']):
            # 确保均线数据的时间与K线数据时间对齐
            df_merged = pd.merge(df[['date']], df_ind[['date', 'ma_5', 'ma_20', 'ma_60']], on='date', how='left')
            
            # 使用日期字符串显示均线（与K线使用相同索引）
            # 严格的数据验证和清理（应用与K线相同的机制）
            ma_columns = ['ma_5', 'ma_20', 'ma_60']
            color_map = {'ma_5': 'orange', 'ma_20': 'blue', 'ma_60': 'red'}
            
            for ma_col in ma_columns:
                if ma_col in df_merged.columns:
                    # 严格的数据验证
                    raw_ma_data = df_merged[ma_col]
                    clean_ma_data = raw_ma_data.dropna()
                    
                    if len(clean_ma_data) > 0:
                        ma_min, ma_max = clean_ma_data.min(), clean_ma_data.max()
                        logger.info(f"{ma_col.upper()}验证: 范围={ma_min:.2f}-{ma_max:.2f}, 有效数据={len(clean_ma_data)}/{len(df_merged)}")
                        logger.info(f"{ma_col.upper()}样本值: {[f'{x:.2f}' for x in clean_ma_data.head(5)]}")
                        logger.info(f"{ma_col.upper()}添加到第1行第1列（价格轴）")
                    
                    # 使用原始数据进行渲染（保持NaN值以确保对齐）
                    fig.add_trace(go.Scatter(
                        x=dates_str,
                        y=raw_ma_data,
                        name=ma_col.upper(),
                        line=dict(color=color_map.get(ma_col, 'black'), width=1.5),
                        connectgaps=False,  # 不连接缺失值
                        legendgroup='ma',
                        legendgrouptitle_text='均线',
                        hovertemplate='日期: %{x}<br>%{y:.2f}<extra></extra>'
                    ), row=1, col=1)

        # === 布林线（Row 1，虚线，默认隐藏，点击图例可显示）===
        if not df_ind.empty:
            boll_cols = [c for c in ['boll_upper', 'boll_middle', 'boll_lower'] if c in df_ind.columns]
            if boll_cols:
                df_boll = pd.merge(df[['date']], df_ind[['date'] + boll_cols], on='date', how='left')
                boll_cfg = {
                    'boll_upper':  ('BOLL上轨', '#e53935'),
                    'boll_middle': ('BOLL中轨', '#fb8c00'),
                    'boll_lower':  ('BOLL下轨', '#1e88e5'),
                }
                for col in boll_cols:
                    if col in df_boll.columns:
                        nm, clr = boll_cfg[col]
                        fig.add_trace(go.Scatter(
                            x=dates_str, y=df_boll[col],
                            name=nm,
                            line=dict(color=clr, width=1.5, dash='dash'),
                            visible='legendonly',
                            legendgroup='boll',
                            legendgrouptitle_text='布林线',
                            hovertemplate=f'日期: %{{x}}<br>{nm}: %{{y:.2f}}<extra></extra>'
                        ), row=1, col=1)

        # === 主力成本线（Row 1，点线，默认隐藏，点击图例可显示）===
        if 'volume' in df.columns and 'close' in df.columns and len(df) >= 20:
            def _major_cost_inline(n):
                tail = df.tail(n)
                total_vol = float(tail['volume'].sum())
                if total_vol == 0:
                    return None
                return round(float((tail['close'] * tail['volume']).sum() / total_vol), 2)

            cost_cfg = [
                ('主力成本(20日)',  '#8e24aa', 20),
                ('主力成本(60日)',  '#0277bd', 60),
                ('主力成本(120日)', '#2e7d32', 120),
            ]
            for cost_name, cost_color, cost_n in cost_cfg:
                if len(df) >= cost_n:
                    cost_val = _major_cost_inline(cost_n)
                    if cost_val is not None:
                        fig.add_trace(go.Scatter(
                            x=dates_str,
                            y=[cost_val] * len(dates_str),
                            name=cost_name,
                            line=dict(color=cost_color, width=1.5, dash='dot'),
                            visible='legendonly',
                            legendgroup='major_cost',
                            legendgrouptitle_text='主力成本',
                            hovertemplate=f'日期: %{{x}}<br>{cost_name}: {cost_val:.2f}<extra></extra>'
                        ), row=1, col=1)

        # 添加成交量柱（Row 2，涨跌着色，单位转换为万手）
        if 'volume' in df.columns:
            volume_colors = ['#ef5350' if c >= o else '#26a69a'
                             for c, o in zip(df['close'], df['open'])]
            volume_wan = [round(v / 10000, 2) if v is not None and not pd.isna(v) else None
                         for v in df['volume'].tolist()]
            fig.add_trace(go.Bar(
                x=dates_str,
                y=volume_wan,
                name='成交量',
                marker_color=volume_colors,
                showlegend=False,
                hovertemplate='日期: %{x}<br>成交量: %{y:.2f}万手<extra></extra>'
            ), row=2, col=1)
            fig.update_yaxes(title_text='万手', row=2, col=1, tickformat=',.1f')

        # 周期显示名称
        freq_names = {'day': '日K', 'week': '周K', 'month': '月K'}
        freq_name = freq_names.get(original_freq, '日K')
        
        # 计算合适的Y轴范围，包含K线和MA线
        if not df.empty:
            price_min = df[['open', 'high', 'low', 'close']].min().min()
            price_max = df[['open', 'high', 'low', 'close']].max().max()
            
            logger.info(f"K线价格范围: {price_min:.2f} - {price_max:.2f}")
            
            # 如果有MA线，也要考虑MA的范围
            if original_freq == 'day' and not df_ind.empty:
                ma_cols = [col for col in df_ind.columns if col.startswith('ma_') and col in ['ma_5', 'ma_20', 'ma_60']]
                if ma_cols:
                    # 过滤掉NaN值后再计算范围
                    ma_data_filtered = df_ind[ma_cols].dropna()
                    if not ma_data_filtered.empty:
                        ma_min = ma_data_filtered.min().min()
                        ma_max = ma_data_filtered.max().max()
                        logger.info(f"MA价格范围: {ma_min:.2f} - {ma_max:.2f}")
                        
                        # 使用更合理的边距计算
                        all_min = min(price_min, ma_min)
                        all_max = max(price_max, ma_max)
                        range_size = all_max - all_min
                        y_min = all_min - range_size * 0.05  # 5%边距
                        y_max = all_max + range_size * 0.05
                    else:
                        y_min = price_min * 0.95
                        y_max = price_max * 1.05
                else:
                    y_min = price_min * 0.95
                    y_max = price_max * 1.05
            else:
                y_min = price_min * 0.95
                y_max = price_max * 1.05
            
            logger.info(f"最终Y轴范围: {y_min:.2f} - {y_max:.2f}")
        else:
            y_min, y_max = 0, 100
        
        fig.update_layout(
            title=f'{stock_name}({code}) {freq_name}线图',
            xaxis_title='交易日',
            height=950,
            xaxis_rangeslider_visible=False,
            showlegend=True
        )
        
        # 设置主图（价格）Y轴范围
        fig.update_yaxes(range=[y_min, y_max], autorange=False, row=1, col=1)
        fig.update_yaxes(title_text='价格', row=1, col=1)
        
        # 如果有指标数据，绘制 MACD（Row 3）和 RSI（Row 4）
        if not df_ind.empty:
            # === MACD (Row 3) ===
            macd_cols = [c for c in ['macd', 'macd_signal', 'macd_histogram'] if c in df_ind.columns]
            if macd_cols:
                df_macd_merged = pd.merge(df[['date']], df_ind[['date'] + macd_cols], on='date', how='left')
                if 'macd' in df_macd_merged.columns and 'macd_signal' in df_macd_merged.columns:
                    fig.add_trace(go.Scatter(
                        x=dates_str, y=df_macd_merged['macd'],
                        name='DIF', line=dict(color='#2196f3', width=1.5),
                        connectgaps=False,
                        hovertemplate='日期: %{x}<br>DIF: %{y:.4f}<extra></extra>'
                    ), row=3, col=1)
                    fig.add_trace(go.Scatter(
                        x=dates_str, y=df_macd_merged['macd_signal'],
                        name='DEA', line=dict(color='#ff9800', width=1.5),
                        connectgaps=False,
                        hovertemplate='日期: %{x}<br>DEA: %{y:.4f}<extra></extra>'
                    ), row=3, col=1)
                if 'macd_histogram' in df_macd_merged.columns:
                    hist_vals = df_macd_merged['macd_histogram']
                    bar_colors = ['#ef5350' if v is not None and not pd.isna(v) and v >= 0 else '#26a69a' for v in hist_vals]
                    fig.add_trace(go.Bar(
                        x=dates_str, y=hist_vals,
                        name='MACD柱', marker_color=bar_colors,
                        showlegend=False,
                        hovertemplate='日期: %{x}<br>MACD: %{y:.4f}<extra></extra>'
                    ), row=3, col=1)
                fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.4, row=3, col=1)
                fig.update_yaxes(title_text='MACD', row=3, col=1)

            # === KDJ (Row 3, hidden by default) ===
            kdj_cols = [c for c in ['kdj_k', 'kdj_d', 'kdj_j'] if c in df_ind.columns]
            if kdj_cols:
                df_kdj = pd.merge(df[['date']], df_ind[['date'] + kdj_cols], on='date', how='left')
                kdj_colors = {'kdj_k': '#ff9800', 'kdj_d': '#2196f3', 'kdj_j': '#e91e63'}
                kdj_names = {'kdj_k': 'K', 'kdj_d': 'D', 'kdj_j': 'J'}
                for col in kdj_cols:
                    if col in df_kdj.columns:
                        fig.add_trace(go.Scatter(
                            x=dates_str, y=df_kdj[col],
                            name=f'KDJ_{kdj_names[col]}', line=dict(color=kdj_colors[col], width=1.5),
                            visible=False, legendgroup='kdj',
                            hovertemplate=f'日期: %{{x}}<br>{kdj_names[col]}: %{{y:.2f}}<extra></extra>'
                        ), row=3, col=1)

            # 布林线已移至 Row 1（K线图），见上方代码

            # === WR (Row 3, hidden by default) ===
            if 'wr_14' in df_ind.columns:
                df_wr = pd.merge(df[['date']], df_ind[['date', 'wr_14']], on='date', how='left')
                fig.add_trace(go.Scatter(
                    x=dates_str, y=df_wr['wr_14'],
                    name='WR_14', line=dict(color='#9c27b0', width=1.5),
                    visible=False, legendgroup='wr',
                    hovertemplate='日期: %{x}<br>WR(14): %{y:.2f}<extra></extra>'
                ), row=3, col=1)

            # === RSI (Row 4) ===
            rsi_cols = [c for c in ['rsi_6', 'rsi_12', 'rsi_24'] if c in df_ind.columns]
            if rsi_cols:
                import numpy as np
                df_rsi_merged = pd.merge(df[['date']], df_ind[['date'] + rsi_cols], on='date', how='left')
                color_map = {'rsi_6': '#ff9800', 'rsi_12': '#2196f3', 'rsi_24': '#9c27b0'}

                # 计算 RSI 百分位参考线（前100天内的第x%和第y%位置）
                rsi_pct_low = float(request.args.get('rsi_pct_low', 5))
                rsi_pct_high = float(request.args.get('rsi_pct_high', 95))
                rsi_ref_col = 'rsi_6' if 'rsi_6' in df_ind.columns else rsi_cols[0]

                # 预排序指标数据，避免循环内反复排序
                df_ind_sorted = df_ind.sort_values('date').reset_index(drop=True)
                rsi_ref_values = df_ind_sorted[rsi_ref_col].values
                rsi_ref_dates = df_ind_sorted['date'].values
                display_dates = df['date'].values

                pct_line_low_vals = []
                pct_line_high_vals = []
                for d_date in display_dates:
                    # 使用 numpy searchsorted 高效查找
                    idx = np.searchsorted(rsi_ref_dates, d_date, side='right')
                    window = rsi_ref_values[max(0, idx - 100):idx]
                    window = window[~np.isnan(window)]
                    if len(window) >= 10:
                        pct_line_low_vals.append(float(np.percentile(window, rsi_pct_low)))
                        pct_line_high_vals.append(float(np.percentile(window, rsi_pct_high)))
                    else:
                        pct_line_low_vals.append(None)
                        pct_line_high_vals.append(None)

                for col_name in rsi_cols:
                    raw_rsi_data = df_rsi_merged[col_name]
                    clean_rsi_data = raw_rsi_data.dropna()
                    pct_col = f"{col_name}_pct_100"
                    pct_list = None
                    if pct_col in df_ind.columns:
                        df_pct = pd.merge(df[['date']], df_ind[['date', pct_col]], on='date', how='left')
                        pct_list = df_pct[pct_col].tolist()
                    
                    # 如果没有预计算的百分位列，基于滚动窗口计算
                    if pct_list is None:
                        col_values = df_ind_sorted[col_name].values
                        merged_dates = df_rsi_merged['date'].values
                        merged_vals = df_rsi_merged[col_name].values
                        pct_list = []
                        for i in range(len(merged_vals)):
                            current_val = merged_vals[i]
                            if np.isnan(current_val):
                                pct_list.append(None)
                                continue
                            idx = np.searchsorted(rsi_ref_dates, merged_dates[i], side='right')
                            window = col_values[max(0, idx - 100):idx]
                            window = window[~np.isnan(window)]
                            if len(window) >= 10:
                                rank = int((window < current_val).sum())
                                pct_list.append(round(rank / len(window) * 100, 2))
                            else:
                                pct_list.append(None)
                    
                    if len(clean_rsi_data) > 0:
                        rsi_min, rsi_max = clean_rsi_data.min(), clean_rsi_data.max()
                        if rsi_min < 0 or rsi_max > 100:
                            clean_rsi_data = clean_rsi_data.clip(0, 100)
                    
                    scatter_kwargs = dict(
                        x=dates_str,
                        y=raw_rsi_data,
                        name=col_name.upper(),
                        line=dict(color=color_map.get(col_name, 'black'), width=1.5),
                        connectgaps=False,
                    )
                    if pct_list is not None:
                        scatter_kwargs['customdata'] = pct_list
                        scatter_kwargs['hovertemplate'] = (
                            '日期: %{x}<br>' + col_name.upper() + ': %{y:.2f}'
                            '<br>位于前100天内的 %{customdata:.1f}% 高<extra></extra>'
                        )
                    else:
                        scatter_kwargs['hovertemplate'] = '日期: %{x}<br>%{y:.2f}<extra></extra>'
                    
                    fig.add_trace(go.Scatter(**scatter_kwargs), row=4, col=1)

                # RSI 百分位参考线
                fig.add_trace(go.Scatter(
                    x=dates_str, y=pct_line_low_vals,
                    name=f'P{rsi_pct_low:.0f}%',
                    line=dict(color='green', width=1, dash='dot'),
                    connectgaps=True, showlegend=True,
                    hovertemplate='日期: %{x}<br>P' + f'{rsi_pct_low:.0f}%' + ': %{y:.2f}<extra></extra>'
                ), row=4, col=1)
                fig.add_trace(go.Scatter(
                    x=dates_str, y=pct_line_high_vals,
                    name=f'P{rsi_pct_high:.0f}%',
                    line=dict(color='red', width=1, dash='dot'),
                    connectgaps=True, showlegend=True,
                    hovertemplate='日期: %{x}<br>P' + f'{rsi_pct_high:.0f}%' + ': %{y:.2f}<extra></extra>'
                ), row=4, col=1)

                # 超买超卖参考线（70/30）
                fig.add_hline(y=70, line_dash="dash", line_color="red", opacity=0.3, row=4, col=1)
                fig.add_hline(y=30, line_dash="dash", line_color="green", opacity=0.3, row=4, col=1)
                fig.update_yaxes(title_text='RSI', row=4, col=1, range=[0, 100], autorange=False)
        
        # 统一X轴刻度与标签（只在底部显示日期标签）
        tick_indices = list(range(0, len(df), max(1, len(df)//10)))
        tickvals_dates = [dates_str[i] for i in tick_indices]
        ticktext = [df.iloc[i]['date'].strftime('%Y-%m-%d') for i in tick_indices]
        fig.update_xaxes(tickmode='array', tickvals=tickvals_dates, ticktext=ticktext, row=4, col=1)
        # 顶部子图隐藏刻度标签
        fig.update_xaxes(showticklabels=False, row=1, col=1)
        fig.update_xaxes(showticklabels=False, row=2, col=1)
        fig.update_xaxes(showticklabels=False, row=3, col=1)
        # 使用 category 类型以避免交易日间隙同时保留日期作为 hover 值
        fig.update_xaxes(type='category', row=1, col=1)
        fig.update_xaxes(type='category', row=2, col=1)
        fig.update_xaxes(type='category', row=3, col=1)
        fig.update_xaxes(type='category', row=4, col=1)
        
        # 准备响应数据
        response_data = json.loads(json.dumps(fig, cls=PlotlyJSONEncoder))

        # Plotly 的 JSON 可能会把大数组编码为二进制（bdata），这在通过 AJAX 传输给前端时
        # 会导致前端不能直接解析。把可能的 bdata 解码为普通列表，确保前端绘图一致。
        try:
            import base64 as _base64
            import numpy as _np
            for trace in response_data.get('data', []):
                for key in ('x', 'y', 'open', 'high', 'low', 'close', 'customdata'):
                    val = trace.get(key)
                    if isinstance(val, dict):
                        # 两种常见结构：{'bdata': '<base64>' , 'dtype': 'float64'} 或 {'b64': '<base64>', 'dtype': 'float64'}
                        b64 = None
                        dtype = val.get('dtype', 'float64')
                        if 'bdata' in val:
                            b64 = val.get('bdata')
                        elif 'b64' in val:
                            b64 = val.get('b64')
                        if b64 is not None:
                            try:
                                arr = _np.frombuffer(_base64.b64decode(b64), dtype=_np.dtype(dtype))
                                trace[key] = arr.tolist()
                            except Exception:
                                # 如果解码失败则保留原始值
                                pass
        except Exception:
            logger.exception('解码 bdata 失败')

        # 将所有可能包含 NaN/inf 的数值替换为 None，以产生合法的 JSON
        try:
            import math as _math
            def _sanitize(v):
                # recursive sanitize
                if isinstance(v, dict):
                    return {k: _sanitize(val) for k, val in v.items()}
                if isinstance(v, list):
                    return [_sanitize(x) for x in v]
                # numpy types might not be float, try to coerce
                try:
                    # exclude booleans
                    if isinstance(v, (float,)):
                        if _math.isnan(v) or _math.isinf(v):
                            return None
                        return v
                except Exception:
                    pass
                return v

            for trace in response_data.get('data', []):
                for key in ('x', 'y', 'open', 'high', 'low', 'close'):
                    if key in trace:
                        trace[key] = _sanitize(trace[key])
        except Exception:
            logger.exception('Sanitize NaN failed')

        # 调试：检查图表数据
        if 'data' in response_data and len(response_data['data']) > 0:
            trace = response_data['data'][0]
            logger.info(f"K线trace类型: {trace.get('type')}, 数据点数: {len(trace.get('open', []))}")
            if trace.get('open'):
                open_data = trace['open']
                if hasattr(open_data, '__getitem__'):
                    # 如果是可索引的对象（列表、数组等）
                    sample = open_data[:3] if len(open_data) >= 3 else open_data
                    logger.info(f"K线open数据样例: {list(sample) if hasattr(sample, '__iter__') else sample}")
                else:
                    logger.info(f"K线open数据类型: {type(open_data)}")

        # 添加指标数据（包括RSI和MA）供前端使用（返回为纯数字以便自动化验证）
        if not df_ind.empty:
            cols = ['date']
            for c in ['rsi_6', 'rsi_12', 'rsi_24']:
                if c in df_ind.columns:
                    cols.append(c)
                    # 同时加入对应的百分位列（如果存在）
                    pct_col = f'{c}_pct_100'
                    if pct_col in df_ind.columns:
                        cols.append(pct_col)
            for c in ['ma_5', 'ma_20', 'ma_60']:
                if c in df_ind.columns:
                    cols.append(c)
            # 取最近200条以供前端显示/验证
            indicators_df = df_ind[cols].copy()
            # 格式化日期为字符串
            indicators_df['date'] = indicators_df['date'].dt.strftime('%Y-%m-%d')
            indicators_data = indicators_df.tail(200).to_dict(orient='records')
            # 处理NaN -> None
            for record in indicators_data:
                for key, val in list(record.items()):
                    if pd.isna(val):
                        record[key] = None
            response_data['indicators'] = indicators_data

            # 同步生成与K线对齐的MA数组（方便程序化校验），按价格df的顺序
            if any(m in df_ind.columns for m in ['ma_5', 'ma_20', 'ma_60']):
                ma_df = pd.merge(df[['date']], df_ind[['date', 'ma_5', 'ma_20', 'ma_60']].copy(), on='date', how='left')
                ma_df['date'] = ma_df['date'].dt.strftime('%Y-%m-%d')
                ma_values = ma_df.to_dict(orient='records')
                # 处理NaN
                for record in ma_values:
                    for key, val in list(record.items()):
                        if pd.isna(val):
                            record[key] = None
                response_data['ma_values'] = ma_values

        # 构建前端指标切换器所需的完整 indicator_data
        if not df_ind.empty:
            def _col_aligned(col_name):
                if col_name not in df_ind.columns:
                    return []
                merged = pd.merge(df[['date']], df_ind[['date', col_name]], on='date', how='left')
                return [None if pd.isna(v) else float(v) for v in merged[col_name]]

            response_data['indicator_data'] = {
                'dates': df['date'].dt.strftime('%Y-%m-%d').tolist(),
                'macd': {
                    'dif': _col_aligned('macd'),
                    'dea': _col_aligned('macd_signal'),
                    'macd': _col_aligned('macd_histogram'),
                },
                'rsi': {
                    'rsi_6': _col_aligned('rsi_6'),
                    'rsi_12': _col_aligned('rsi_12'),
                    'rsi_24': _col_aligned('rsi_24'),
                },
                'volume': [None if pd.isna(v) else float(v) for v in df['volume']] if 'volume' in df.columns else [],
                'ma': {
                    'ma5': _col_aligned('ma_5'),
                    'ma20': _col_aligned('ma_20'),
                    'ma60': _col_aligned('ma_60'),
                },
            }

        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"生成图表失败 {code}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/strategies')
def api_strategies():
    """获取策略列表，返回内部 key 与显示名称"""
    strategy_manager.reload_from_file()  # 热重载：自动同步磁盘上的策略变更
    strategies = []
    for key in strategy_manager.list_strategies():
        strat = strategy_manager.get_strategy(key)
        has_buy = any(getattr(r, 'action', '') == 'buy' for r in strat.rules) if strat else False
        has_sell = any(getattr(r, 'action', '') == 'sell' for r in strat.rules) if strat else False
        strategies.append({
            'key': key,
            'name': strat.name if strat else key,
            'exclusion_rules_count': len(strat.exclusion_rules) if strat else 0,
            'has_buy': has_buy,
            'has_sell': has_sell,
        })
    return jsonify(strategies)


@app.route('/api/strategy/<name>')
def api_strategy_detail(name):
    """获取策略详情（支持按 key 或 name 查找）"""
    strategy_manager.reload_from_file()  # 热重载
    actual_key = name
    strategy = strategy_manager.strategies.get(name)
    if not strategy:
        for key, s in strategy_manager.strategies.items():
            if hasattr(s, 'name') and s.name == name:
                strategy = s
                actual_key = key
                break
    if not strategy:
        return jsonify({'error': 'Strategy not found'}), 404
    
    result = strategy.to_dict()
    result['key'] = actual_key
    return jsonify(result)


@app.route('/api/strategy/run', methods=['POST'])
def api_run_strategy():
    """运行策略（支持独立的买入策略和卖出策略）"""
    try:
        data = request.json
        code = data.get('code')
        buy_strategy_name = data.get('buy_strategy') or data.get('strategy')
        sell_strategy_name = data.get('sell_strategy') or buy_strategy_name

        if not code or not buy_strategy_name:
            return jsonify({'error': 'Missing code or strategy'}), 400

        if sell_strategy_name and sell_strategy_name != buy_strategy_name:
            decision = strategy_manager.run_strategy_split(buy_strategy_name, sell_strategy_name, code)
        else:
            decision = strategy_manager.run_strategy(buy_strategy_name, code)

        return jsonify({
            'code': decision.code,
            'action': decision.action,
            'position_ratio': decision.position_ratio,
            'confidence': decision.confidence,
            'reasoning': decision.reasoning,
            'timestamp': decision.timestamp,
        })

    except Exception as e:
        logger.error(f"运行策略失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/backtest/run', methods=['POST'])
def api_run_backtest():
    """运行回测（异步模式）：启动后台线程并返回 run_id；前端可通过 SSE 订阅进度并在完成后拉取结果。"""
    try:
        strategy_manager.reload_from_file()  # 热重载，确保最新策略可用
        data = request.json
        code = data.get('code')
        buy_strategy_name = data.get('buy_strategy') or data.get('strategy')
        sell_strategy_name = data.get('sell_strategy') or buy_strategy_name
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        initial_capital = data.get('initial_capital', 1000000)

        if not all([code, buy_strategy_name, start_date, end_date]):
            return jsonify({'error': 'Missing required parameters'}), 400

        buy_strategy = strategy_manager.get_strategy(buy_strategy_name)
        if not buy_strategy:
            return jsonify({'error': f'买入策略不存在: {buy_strategy_name}'}), 404

        if sell_strategy_name and sell_strategy_name != buy_strategy_name:
            sell_strategy = strategy_manager.get_strategy(sell_strategy_name)
            if not sell_strategy:
                return jsonify({'error': f'卖出策略不存在: {sell_strategy_name}'}), 404
            from .strategy import merge_buy_sell_strategies
            strategy = merge_buy_sell_strategies(buy_strategy, sell_strategy)
        else:
            strategy = buy_strategy

        # 生成 run_id 并启动后台线程执行回测
        import threading, uuid, traceback as _tb, time as _time
        run_id = str(uuid.uuid4())
        BACKTEST_PROGRESS[run_id] = {'processed': 0, 'total': None, 'elapsed': 0, 'eta': None, 'status': 'running'}

        def _worker():
            try:
                t0 = _time.time()
                def _progress_cb(info):
                    try:
                        # 合并进度信息
                        BACKTEST_PROGRESS[run_id].update(info)
                    except Exception:
                        logger.exception('更新 BACKTEST_PROGRESS 失败')

                result = backtest_engine.run_backtest(code, strategy, start_date, end_date, initial_capital, progress_callback=_progress_cb)
                duration = _time.time() - t0

                # 构造可序列化的结果
                def _to_primitive(v):
                    import math as _math, numpy as _np, pandas as _pd
                    try:
                        if v is None: return None
                        if isinstance(v, bool): return v
                        if isinstance(v, (int, float, str)):
                            if isinstance(v, float) and (_math.isnan(v) or _math.isinf(v)): return None
                            return v
                        if isinstance(v, _np.integer): return int(v)
                        if isinstance(v, _np.floating):
                            fv = float(v)
                            if _math.isnan(fv) or _math.isinf(fv): return None
                            return fv
                        if isinstance(v, _pd.Timestamp): return v.strftime('%Y-%m-%d %H:%M:%S')
                        if isinstance(v, _pd.Timedelta): return str(v)
                        if hasattr(v, 'item'):
                            try:
                                item = v.item();
                                if isinstance(item, float) and (_math.isnan(item) or _math.isinf(item)): return None
                                return item
                            except Exception:
                                pass
                    except Exception:
                        pass
                    return v

                response = {
                    'code': _to_primitive(result.code),
                    'strategy_name': _to_primitive(result.strategy_name),
                    'start_date': _to_primitive(result.start_date),
                    'end_date': _to_primitive(result.end_date),
                    'initial_capital': _to_primitive(result.initial_capital),
                    'final_capital': _to_primitive(result.final_capital),
                    'total_return_pct': _to_primitive(result.total_return_pct),
                    'deployed_return_pct': _to_primitive(result.deployed_return_pct),
                    'max_position_ratio': _to_primitive(result.max_position_ratio),
                    'annual_return': _to_primitive(result.annual_return),
                    'max_drawdown_pct': _to_primitive(result.max_drawdown_pct),
                    'sharpe_ratio': _to_primitive(result.sharpe_ratio),
                    'total_trades': _to_primitive(result.total_trades),
                    'win_rate': _to_primitive(result.win_rate),
                    'profit_factor': _to_primitive(result.profit_factor),
                    'duration_seconds': round(duration, 2),
                    'trades': [{
                        'date': _to_primitive(t.date),
                        'action': _to_primitive(t.action),
                        'shares': _to_primitive(t.shares),
                        'price': _to_primitive(t.price),
                        'amount': _to_primitive(t.amount),
                        'reason': _to_primitive(t.reason),
                    } for t in result.trades],
                    # 将权益曲线序列化为可保存/绘图的结构
                    'equity_curve': []
                }
                # 加权平均资金持有率（元/天）
                try:
                    wcpd = _calc_weighted_capital_per_day(response['trades'])
                    response['weighted_capital_per_day'] = round(wcpd, 2) if wcpd is not None else None
                except Exception:
                    response['weighted_capital_per_day'] = None
                try:
                    if getattr(result, 'equity_curve', None) is not None:
                        # equity_curve 可能为 DataFrame
                        ec = result.equity_curve.copy()
                        # 确保日期序列化为字符串
                        if hasattr(ec, 'to_dict'):
                            ec_records = ec.to_dict(orient='records')
                        else:
                            ec_records = list(ec)
                        for r in ec_records:
                            rec = {}
                            for k, v in r.items():
                                try:
                                    if hasattr(v, 'strftime'):
                                        rec[k] = v.strftime('%Y-%m-%d')
                                    else:
                                        rec[k] = _to_primitive(v)
                                except Exception:
                                    rec[k] = _to_primitive(v)
                            response['equity_curve'].append(rec)
                except Exception:
                    logger.exception('序列化 equity_curve 失败')

                # 保存结果到文件，便于后续读取与审计
                try:
                    out_dir = os.path.join(config.get('data_storage.data_dir', './data'), 'backtests')
                    os.makedirs(out_dir, exist_ok=True)
                    out_path = os.path.join(out_dir, f'backtest_{run_id}.json')
                    with open(out_path, 'w', encoding='utf-8') as f:
                        import json as _json
                        _json.dump({'run_id': run_id, 'result': response}, f, ensure_ascii=False, indent=2)
                    # 记录文件路径
                    response['_file'] = out_path
                except Exception:
                    logger.exception('保存回测结果到文件失败')

                BACKTEST_RESULTS[run_id] = {'status': 'done', 'result': response}
                BACKTEST_PROGRESS[run_id].update({'processed': BACKTEST_PROGRESS[run_id].get('total') or 0, 'elapsed': round(duration,2), 'eta': 0, 'status': 'done'})
                logger.info(f"回测后台任务完成: run_id={run_id}, code={code}, strategy={strategy.name}, trades={len(result.trades)}, duration={duration:.1f}s")
                
                # 触发一次写回文件的动作（确保持久化）
                try:
                    out_dir = os.path.join(config.get('data_storage.data_dir', './data'), 'backtests')
                    os.makedirs(out_dir, exist_ok=True)
                    out_path = os.path.join(out_dir, f'backtest_{run_id}.json')
                    import json as _json
                    with open(out_path, 'w', encoding='utf-8') as f:
                        _json.dump({'run_id': run_id, 'result': response}, f, ensure_ascii=False, indent=2)
                except Exception:
                    logger.exception('再次保存回测结果到文件失败')
            except Exception as e:
                tb = _tb.format_exc()
                BACKTEST_RESULTS[run_id] = {'status': 'error', 'error': str(e), 'traceback': tb}
                BACKTEST_PROGRESS[run_id].update({'status': 'error', 'error': str(e)})
                logger.exception(f"后台回测任务失败: run_id={run_id}, error={e}\n{tb}")

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()

        # 返回 run_id（202 Accepted）
        return jsonify({'run_id': run_id}), 202

    except Exception as e:
        logger.exception(f"回测失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/backtest/progress')
def api_backtest_progress():
    """SSE 端点：订阅回测进度。参数 run_id 必须提供。"""
    from flask import Response
    run_id = request.args.get('run_id')
    if not run_id:
        return jsonify({'error': 'Missing run_id'}), 400
    
    def gen():
        import time as _time, json as _json
        last_status = None
        while True:
            info = BACKTEST_PROGRESS.get(run_id)
            if info is None:
                # 任务未找到，发送 end
                yield f"data: {_json.dumps({'status': 'not_found'})}\n\n"
                break
            # always send current info
            try:
                yield f"data: {_json.dumps(info)}\n\n"
            except Exception:
                yield f"data: {{'status':'error','message':'serialize_failed'}}\n\n"
            if info.get('status') in ('done', 'error'):
                break
            _time.sleep(1)
    return Response(gen(), mimetype='text/event-stream')


@app.route('/api/backtest/result')
def api_backtest_result():
    run_id = request.args.get('run_id')
    if not run_id:
        return jsonify({'error': 'Missing run_id'}), 400
    res = BACKTEST_RESULTS.get(run_id)
    if res is None:
        return jsonify({'status': 'running'}), 202
    return jsonify(res)


def _calc_weighted_capital_per_day(trades: list):
    """
    计算加权平均资金持有率（元/天）。

    用户定义的公式：
      对每笔买→卖配对（FIFO）：value_i = A_i / D_i（元/天）
      权重 w_i = A_i / Σ(A_j)
      结果 = Σ(w_i × value_i) = Σ(A_i² / D_i) / Σ(A_i)

    Returns: float（元/天），无有效配对时返回 None。
    """
    from datetime import datetime as _dt
    from collections import deque

    def _parse_date(s):
        s = str(s).strip()[:10]  # 取 YYYY-MM-DD 部分
        for fmt in ('%Y-%m-%d', '%Y%m%d'):
            try:
                return _dt.strptime(s, fmt)
            except ValueError:
                continue
        return None

    buy_queue = deque()   # elements: [date_obj, amount_remaining]
    numerator   = 0.0     # Σ(A_i² / D_i)
    denominator = 0.0     # Σ(A_i)

    for t in trades:
        action = t.get('action', '')
        amount = float(t.get('amount') or 0)
        date   = _parse_date(t.get('date', ''))
        if date is None or amount <= 0:
            continue

        if action == 'buy':
            buy_queue.append([date, amount])
        elif action == 'sell':
            remaining = amount
            while remaining > 1e-6 and buy_queue:
                buy_date, buy_amt = buy_queue[0]
                days = (date - buy_date).days
                if days <= 0:
                    days = 1   # same-day round-trip counts as 1 day
                matched = min(buy_amt, remaining)
                numerator   += matched * matched / days
                denominator += matched
                remaining   -= matched
                if buy_amt <= matched + 1e-6:
                    buy_queue.popleft()
                else:
                    buy_queue[0][1] -= matched

    if denominator <= 0:
        return None
    return numerator / denominator

def _get_market_index_code(stock_code):
    """根据股票代码后缀判断对应大盘指数代码和名称"""
    if not stock_code:
        return '000001.SH', '上证指数'
    upper = stock_code.upper()
    if upper.endswith('.HK'):
        return 'HSI.HK', '恒生指数'
    if upper.endswith('.SZ'):
        return '399001.SZ', '深证成指'
    return '000001.SH', '上证指数'


@app.route('/api/backtest/chart', methods=['POST'])
def api_backtest_chart():
    """获取回测图表。支持传入 run_id（优先从已保存的回测结果读取），否则会同步运行回测（回退）。"""
    try:
        data = request.json or {}
        run_id = data.get('run_id')
        code = data.get('code')
        strategy_name = data.get('strategy') or data.get('buy_strategy')
        buy_strategy_name = data.get('buy_strategy') or strategy_name
        sell_strategy_name = data.get('sell_strategy') or buy_strategy_name
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        # 如果提供 run_id，优先从 BACKTEST_RESULTS 或文件读取已完成的回测结果
        result_data = None
        if run_id:
            entry = BACKTEST_RESULTS.get(run_id)
            if entry and entry.get('status') == 'done':
                result_data = entry.get('result')
            else:
                # 尝试从文件加载
                try:
                    out_dir = os.path.join(config.get('data_storage.data_dir', './data'), 'backtests')
                    out_path = os.path.join(out_dir, f'backtest_{run_id}.json')
                    if os.path.exists(out_path):
                        import json as _json
                        with open(out_path, 'r', encoding='utf-8') as f:
                            loaded = _json.load(f)
                            result_data = loaded.get('result')
                except Exception:
                    logger.exception('从文件加载回测结果失败')

        # 如果没有 run_id 或未能加载已保存结果，则回退到同步运行回测（尽量避免）
        if result_data is None:
            buy_strat = strategy_manager.get_strategy(buy_strategy_name)
            if not buy_strat:
                return jsonify({'error': '策略不存在'}), 404
            if sell_strategy_name and sell_strategy_name != buy_strategy_name:
                sell_strat = strategy_manager.get_strategy(sell_strategy_name)
                from .strategy import merge_buy_sell_strategies
                strategy = merge_buy_sell_strategies(buy_strat, sell_strat) if sell_strat else buy_strat
            else:
                strategy = buy_strat
            result = backtest_engine.run_backtest(code, strategy, start_date, end_date)
            # 将 DataFrame 序列化为列表
            ec = result.equity_curve
            if ec is None or ec.empty:
                return jsonify({'error': 'No equity curve data'}), 404
            # prepare a similar structure as saved result
            result_data = {
                'equity_curve': []
            }
            ec_records = ec.to_dict(orient='records')
            for r in ec_records:
                rec = {}
                for k, v in r.items():
                    try:
                        if hasattr(v, 'strftime'):
                            rec[k] = v.strftime('%Y-%m-%d')
                        else:
                            rec[k] = v
                    except Exception:
                        rec[k] = v
                result_data['equity_curve'].append(rec)
            # also include trades if possible
            if hasattr(result, 'trades'):
                result_data['trades'] = [{
                    'date': t.date,
                    'action': t.action,
                    'price': t.price,
                    'shares': t.shares,
                    'amount': t.amount
                } for t in result.trades]

        # 构建图表：包括价格曲线（equity_curve 中的 price）和买卖标记（含注释）
        ec_list = result_data.get('equity_curve', [])
        if not ec_list:
            return jsonify({'error': 'No equity curve data'}), 404

        dates = [str(r.get('date')) for r in ec_list]
        equities = [r.get('equity') for r in ec_list]
        prices = [r.get('price') for r in ec_list]
        # Per-point hover: action + reason from equity curve
        _action_label = {'buy': '买入', 'sell': '卖出', 'hold': '观望'}
        hover_texts = []
        for r in ec_list:
            act = r.get('signal_action', '')
            rsn = r.get('signal_reason', '')
            label = _action_label.get(act, act)
            hover_texts.append(f'{label}: {rsn}' if rsn else label)

        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            row_heights=[0.5, 0.25, 0.25],
            vertical_spacing=0.03,
            subplot_titles=('股价走势', '累计收益(%)', '大盘走势')
        )

        # Row 1: stock price curve with per-point signal hover
        fig.add_trace(go.Scatter(
            x=dates, y=prices, name='股价',
            line=dict(color='black', width=1),
            customdata=hover_texts,
            hovertemplate='日期: %{x}<br>股价: %{y:.2f}<br>%{customdata}<extra></extra>'
        ), row=1, col=1)

        # Buy/sell markers on price chart
        trades = result_data.get('trades', [])
        buy_x, buy_y, buy_text = [], [], []
        sell_x, sell_y, sell_text = [], [], []
        for t in trades:
            tx = t.get('date')
            tp = t.get('price')
            txt = ''
            if t.get('shares') is not None:
                txt += f"数量: {t.get('shares')}"
            if t.get('reason'):
                txt += ("; " if txt else "") + str(t.get('reason'))
            if t.get('action') == 'buy':
                buy_x.append(tx); buy_y.append(tp); buy_text.append(txt or '买入')
            elif t.get('action') == 'sell':
                sell_x.append(tx); sell_y.append(tp); sell_text.append(txt or '卖出')
        if buy_x:
            fig.add_trace(go.Scatter(
                x=buy_x, y=buy_y, mode='markers', name='买入',
                marker=dict(symbol='triangle-up', color='green', size=12),
                hovertext=buy_text, hovertemplate='%{hovertext}<extra></extra>'
            ), row=1, col=1)
        if sell_x:
            fig.add_trace(go.Scatter(
                x=sell_x, y=sell_y, mode='markers', name='卖出',
                marker=dict(symbol='triangle-down', color='red', size=12),
                hovertext=sell_text, hovertemplate='%{hovertext}<extra></extra>'
            ), row=1, col=1)

        # Row 2: cumulative return % — two lines: total-capital basis and deployed-capital basis
        initial_capital = next((e for e in equities if e is not None), 1000000)
        max_pos_ratio = result_data.get('max_position_ratio', 1.0) or 1.0
        deployed_capital = initial_capital * max_pos_ratio
        cumulative_returns = [
            (e / initial_capital - 1) * 100 if e is not None else None
            for e in equities
        ]
        fig.add_trace(go.Scatter(
            x=dates, y=cumulative_returns, name='总资金收益(%)',
            line=dict(color='#26a69a', width=2),
            fill='tozeroy',
            fillcolor='rgba(38,166,154,0.1)',
            hovertemplate='日期: %{x}<br>总资金收益: %{y:.2f}%<extra></extra>'
        ), row=2, col=1)
        # Only show deployed-capital line when max_position_ratio < 1 (otherwise identical)
        if max_pos_ratio < 0.999:
            deployed_returns = [
                (e - initial_capital) / deployed_capital * 100 if e is not None else None
                for e in equities
            ]
            fig.add_trace(go.Scatter(
                x=dates, y=deployed_returns,
                name=f'实际仓位收益(% · 上限{int(max_pos_ratio*100)}%资金)',
                line=dict(color='#ef5350', width=2, dash='dot'),
                hovertemplate='日期: %{x}<br>实际仓位收益: %{y:.2f}%<extra></extra>'
            ), row=2, col=1)
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5, row=2, col=1)

        # Row 3: market index trend (沪指/深指/恒生指数 depending on stock exchange)
        index_code, index_name = _get_market_index_code(code)
        try:
            # dates[] is in YYYY-MM-DD; convert to YYYYMMDD for the data fetch
            fetch_start = dates[0].replace('-', '') if dates else ''
            fetch_end = dates[-1].replace('-', '') if dates else ''
            if fetch_start and fetch_end:
                idx_df = unified_data.get_historical_data(index_code, fetch_start, fetch_end)
                if idx_df is not None and not idx_df.empty:
                    idx_df = idx_df.sort_values('date').copy()
                    # Normalize dates to YYYY-MM-DD to match equity curve x-axis
                    idx_df['date_str'] = idx_df['date'].astype(str).str.replace(
                        r'^(\d{4})(\d{2})(\d{2})$', r'\1-\2-\3', regex=True
                    )
                    # Only include dates within the equity curve range
                    idx_df = idx_df[
                        (idx_df['date_str'] >= dates[0]) &
                        (idx_df['date_str'] <= dates[-1])
                    ]
                    fig.add_trace(go.Scatter(
                        x=idx_df['date_str'].tolist(),
                        y=idx_df['close'].tolist(),
                        name=index_name,
                        line=dict(color='#1565C0', width=1.5),
                        hovertemplate='日期: %{x}<br>' + index_name + ': %{y:.2f}<extra></extra>'
                    ), row=3, col=1)
        except Exception as _idx_ex:
            logger.warning(f"获取大盘数据失败 {index_code}: {_idx_ex}")

        fig.update_layout(
            title=f'{code} 回测 - 价格与累计收益',
            height=800,
            showlegend=True
        )
        fig.update_yaxes(title_text='股价', row=1, col=1)
        fig.update_yaxes(title_text='累计收益(%)', row=2, col=1)
        fig.update_yaxes(title_text=index_name, row=3, col=1)

        return jsonify(json.loads(json.dumps(fig, cls=PlotlyJSONEncoder)))

    except Exception as e:
        logger.exception(f"生成回测图表失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/backtest/market_return')
def api_backtest_market_return():
    """计算同期大盘指数收益率，用于与策略回测结果对比"""
    try:
        code = request.args.get('code', '')
        start_date = request.args.get('start_date', '').replace('-', '')
        end_date = request.args.get('end_date', '').replace('-', '')
        if not code or not start_date or not end_date:
            return jsonify({'error': '缺少参数'}), 400

        index_code, index_name = _get_market_index_code(code)
        idx_df = unified_data.get_historical_data(index_code, start_date, end_date)
        if idx_df is None or idx_df.empty:
            return jsonify({'error': f'无法获取 {index_name} 数据'}), 404

        # Filter to the requested date range (data_source may return full history)
        idx_df = idx_df.sort_values('date')
        idx_df = idx_df[
            (idx_df['date'].astype(str) >= start_date) &
            (idx_df['date'].astype(str) <= end_date)
        ]
        if idx_df.empty:
            return jsonify({'error': f'{index_name} 在该时间段内无数据'}), 404

        first_close = float(idx_df['close'].iloc[0])
        last_close = float(idx_df['close'].iloc[-1])
        if first_close == 0:
            return jsonify({'error': '指数起始价格为0'}), 500

        index_return_pct = (last_close / first_close - 1) * 100
        return jsonify({
            'index_code': index_code,
            'index_name': index_name,
            'index_return_pct': round(index_return_pct, 2),
            'start_close': round(first_close, 2),
            'end_close': round(last_close, 2),
        })
    except Exception as e:
        logger.exception(f"计算大盘收益失败: {e}")
        return jsonify({'error': str(e)}), 500



@app.route('/api/risk/portfolio')
def api_risk_portfolio():
    """获取风险组合信息（包含今日盈亏）"""
    try:
        # 尝试从实时数据更新持仓的今日盈亏
        for code, position in risk_manager.positions.items():
            prev_close = 0.0
            # 从实时快照获取昨收价
            for key, info in realtime_snapshot.items():
                # key 现在是统一后缀格式（如 000001.SH）
                # 提取纯代码用于匹配持仓
                bare_code = key.split('.')[0] if '.' in key else key
                if code == key or code == bare_code:
                    # lastPrice = 前一交易日收盘价 (由 data_source.get_realtime_data 设置)
                    # close/now/price = 当日最新价，不能用于今日盈亏基准
                    prev_close = float(info.get('lastPrice', 0) or 0)
                    current_rt_price = float(info.get('now', info.get('price', 0)) or 0)
                    if current_rt_price > 0:
                        risk_manager.update_position(
                            code=position.code,
                            shares=position.shares,
                            avg_cost=position.avg_cost,
                            current_price=current_rt_price,
                            prev_close=prev_close
                        )
                    break

        portfolio_risk = risk_manager.get_portfolio_risk()
        return jsonify(portfolio_risk)
    except Exception as e:
        logger.error(f"获取风险信息失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/risk/positions')
def api_risk_positions():
    """获取持仓信息"""
    try:
        positions_df = risk_manager.get_position_summary()
        if positions_df.empty:
            return jsonify([])
        return jsonify(positions_df.to_dict(orient='records'))
    except Exception as e:
        logger.error(f"获取持仓信息失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/risk/capital', methods=['GET', 'POST'])
def api_risk_capital():
    """获取或更新资金信息"""
    try:
        if request.method == 'GET':
            return jsonify({
                'total_capital': risk_manager.total_capital,
                'available_cash': risk_manager.available_cash,
            })
        
        elif request.method == 'POST':
            data = request.json
            total_capital = data.get('total_capital')
            
            if total_capital is not None:
                risk_manager.total_capital = float(total_capital)
                # Recalculate available_cash = total_capital - total_position_value
                total_position_value = sum(
                    p.shares * p.current_price
                    for p in risk_manager.positions.values()
                )
                risk_manager.available_cash = max(0.0, float(total_capital) - total_position_value)
            
            # 保存状态
            save_system_state()
            
            return jsonify({
                'success': True,
                'total_capital': risk_manager.total_capital,
                'available_cash': risk_manager.available_cash,
            })
    except Exception as e:
        logger.error(f"操作资金信息失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/risk/position', methods=['POST', 'DELETE'])
def api_risk_position():
    """添加、更新或删除持仓"""
    try:
        if request.method == 'POST':
            data = request.json
            code = data.get('code')
            shares = int(data.get('shares', 0))
            avg_cost = float(data.get('avg_cost', 0))
            current_price = float(data.get('current_price', avg_cost))
            
            if not code:
                return jsonify({'error': '股票代码不能为空'}), 400
            
            # 更新持仓
            risk_manager.update_position(code, shares, avg_cost, current_price)
            
            # Recalculate available_cash dynamically
            total_position_value = sum(
                p.shares * p.current_price
                for p in risk_manager.positions.values()
            )
            risk_manager.available_cash = max(0.0, risk_manager.total_capital - total_position_value)
            
            # 保存状态
            save_system_state()
            
            return jsonify({
                'success': True,
                'message': f'持仓 {code} 已更新',
                'position': {
                    'code': code,
                    'shares': shares,
                    'avg_cost': avg_cost,
                    'current_price': current_price,
                }
            })
        
        elif request.method == 'DELETE':
            data = request.json
            code = data.get('code')
            
            if not code:
                return jsonify({'error': '股票代码不能为空'}), 400
            
            # 删除持仓
            if code in risk_manager.positions:
                del risk_manager.positions[code]
                save_system_state()
                return jsonify({
                    'success': True,
                    'message': f'持仓 {code} 已删除'
                })
            else:
                return jsonify({'error': '持仓不存在'}), 404
                
    except Exception as e:
        logger.error(f"操作持仓失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/risk/settings', methods=['GET', 'POST'])
def api_risk_settings():
    """获取或更新风控设置"""
    try:
        if request.method == 'GET':
            return jsonify({
                'max_position_ratio': risk_manager.max_position_ratio,
                'max_single_stock_ratio': risk_manager.max_single_stock_ratio,
                'stop_loss_ratio': risk_manager.stop_loss_ratio,
                'take_profit_ratio': risk_manager.take_profit_ratio,
            })
        
        elif request.method == 'POST':
            data = request.json
            
            if 'max_position_ratio' in data:
                risk_manager.max_position_ratio = float(data['max_position_ratio'])
            if 'max_single_stock_ratio' in data:
                risk_manager.max_single_stock_ratio = float(data['max_single_stock_ratio'])
            if 'stop_loss_ratio' in data:
                risk_manager.stop_loss_ratio = float(data['stop_loss_ratio'])
            if 'take_profit_ratio' in data:
                risk_manager.take_profit_ratio = float(data['take_profit_ratio'])
            
            # 保存状态
            save_system_state()
            
            return jsonify({
                'success': True,
                'settings': {
                    'max_position_ratio': risk_manager.max_position_ratio,
                    'max_single_stock_ratio': risk_manager.max_single_stock_ratio,
                    'stop_loss_ratio': risk_manager.stop_loss_ratio,
                    'take_profit_ratio': risk_manager.take_profit_ratio,
                }
            })
    except Exception as e:
        logger.error(f"操作风控设置失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/news/<code>')
def api_news(code):
    """获取新闻数据"""
    try:
        # 获取股票信息
        stock = stock_manager.get_stock_by_code(code)
        if not stock:
            return jsonify({'error': 'Stock not found'}), 404
        
        # 尝试加载已采集的新闻
        df = news_collector.load_news(code)
        
        # 如果没有新闻数据，尝试实时采集
        if df.empty:
            logger.info(f"未找到 {code} 的新闻数据，尝试实时采集...")
            try:
                df = news_collector.fetch_stock_news(code, max_pages=3)
                if not df.empty:
                    # 进行情感分析
                    df = sentiment_analyzer.analyze_news_df(df)
            except Exception as e:
                logger.warning(f"实时采集新闻失败: {e}")
        
        if df.empty:
            return jsonify([])
        
        # 限制返回最新的50条，并确保字段完整
        df = df.tail(50)
        
        # 转换为字典列表，处理NaN值，并归一化URL为绝对地址
        records = df.to_dict(orient='records')
        for record in records:
            for key, value in list(record.items()):
                if pd.isna(value):
                    record[key] = None
            # 归一化 url 字段为绝对地址，便于前端直接跳转
            url = record.get('url') or ''
            if url:
                try:
                    url = str(url).strip()
                    if url.startswith('..'):
                        url = 'https://vip.stock.finance.sina.com.cn' + url[2:]
                    elif url.startswith('/'):
                        url = 'https://vip.stock.finance.sina.com.cn' + url
                    elif url.startswith('http'):
                        pass
                    else:
                        url = 'https://vip.stock.finance.sina.com.cn/' + url.lstrip('./')
                except Exception:
                    # 如果归一化失败，保留原始值
                    pass
                record['url'] = url
        
        return jsonify(records)
    except Exception as e:
        logger.error(f"获取新闻失败 {code}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/sentiment/<code>')
def api_sentiment(code):
    """获取情感分析数据"""
    try:
        daily_sentiment = sentiment_analyzer.get_daily_sentiment(code)
        if daily_sentiment.empty:
            return jsonify([])
        
        return jsonify(daily_sentiment.to_dict(orient='records'))
    except Exception as e:
        logger.error(f"获取情感分析失败 {code}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/features/<code>')
def api_features(code):
    """获取特征分析"""
    try:
        features = feature_extractor.load_features(code)
        if not features:
            features = feature_extractor.analyze_with_ai(code)
            feature_extractor.save_features(code, features)
        
        return app.response_class(
            response=json.dumps(features, ensure_ascii=False, allow_nan=False, default=str),
            mimetype='application/json'
        )
    except ValueError:
        # allow_nan=False raises ValueError on NaN/Infinity; sanitize and retry
        import math
        def _sanitize(obj):
            if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            if isinstance(obj, dict):
                return {k: _sanitize(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [_sanitize(v) for v in obj]
            return obj
        features = _sanitize(features)
        return app.response_class(
            response=json.dumps(features, ensure_ascii=False, default=str),
            mimetype='application/json'
        )
    except Exception as e:
        logger.error(f"获取特征分析失败 {code}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ai/decision', methods=['POST'])
def api_ai_decision():
    """获取AI决策"""
    try:
        data = request.json
        code = data.get('code')
        strategy_description = data.get('strategy_description')
        
        if not code:
            return jsonify({'error': 'Missing code'}), 400
        
        decision = ai_decision_maker.make_decision(code, strategy_description)
        
        logger.info(f"AI决策结果: code={decision.code}, action={decision.action}, pos={decision.position_ratio}, conf={decision.confidence}")
        logger.debug(f"AI决策理由（前200）: {decision.reasoning[:200]}")
        
        return jsonify({
            'code': decision.code,
            'action': decision.action,
            'position_ratio': decision.position_ratio,
            'confidence': decision.confidence,
            'reasoning': decision.reasoning,
            'timestamp': decision.timestamp,
        })
        
    except Exception as e:
        logger.exception(f"AI决策失败: {e}")
        return jsonify({'error': str(e)}), 500


# ============== 策略管理API ==============

@app.route('/api/strategy/create', methods=['POST'])
def api_create_strategy():
    """创建新策略"""
    try:
        data = request.json
        name = data.get('name')
        description = data.get('description')
        rules = data.get('rules', [])
        
        if not name:
            return jsonify({'error': '策略名称不能为空'}), 400
        
        from .strategy import QuantStrategy, StrategyRule
        
        # 创建策略
        strategy = QuantStrategy(name=name, description=description)
        
        # 设置最大仓位
        max_position_ratio = data.get('max_position_ratio')
        if max_position_ratio is not None:
            strategy.max_position_ratio = max(0.0, min(1.0, float(max_position_ratio)))
        
        # 添加规则
        for rule_data in rules:
            strategy.add_rule(
                condition=rule_data.get('condition', ''),
                action=rule_data.get('action', 'hold'),
                position_ratio=float(rule_data.get('position_ratio', 1.0)),
                reason=rule_data.get('reason', ''),
                connector=rule_data.get('connector', 'OR')
            )
        
        # 添加排除规则
        exclusion_rules = data.get('exclusion_rules', [])
        for exc_data in exclusion_rules:
            strategy.add_exclusion_rule(
                condition=exc_data.get('condition', ''),
                reason=exc_data.get('reason', ''),
                connector=exc_data.get('connector', 'OR')
            )
        
        # 保存到管理器
        strategy.market_regime = data.get('market_regime', [])
        strategy_manager.add_strategy(name, strategy)
        
        # 保存策略到文件
        save_strategies_to_file()
        
        return jsonify({
            'success': True,
            'message': f'策略 {name} 创建成功',
            'strategy': strategy.to_dict()
        })
        
    except Exception as e:
        logger.error(f"创建策略失败: {e}")
        return jsonify({'error': str(e)}), 500


def _resolve_strategy_key(name):
    """解析策略key：先按key查，再按显示名查"""
    if name in strategy_manager.strategies:
        return name
    for key, s in strategy_manager.strategies.items():
        if hasattr(s, 'name') and s.name == name:
            return key
    return None


@app.route('/api/strategy/<name>', methods=['DELETE'])
def api_delete_strategy(name):
    """删除策略"""
    try:
        actual_key = _resolve_strategy_key(name)
        if not actual_key:
            return jsonify({'error': '策略不存在'}), 404
        
        del strategy_manager.strategies[actual_key]
        
        save_strategies_to_file()
        
        return jsonify({
            'success': True,
            'message': f'策略 {name} 已删除'
        })
        
    except Exception as e:
        logger.error(f"删除策略失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/strategy/<name>/update', methods=['POST'])
def api_update_strategy(name):
    """更新策略"""
    try:
        actual_key = _resolve_strategy_key(name)
        if not actual_key:
            return jsonify({'error': '策略不存在'}), 404
        
        data = request.json
        new_display_name = data.get('name')
        description = data.get('description')
        rules = data.get('rules')
        exclusion_rules = data.get('exclusion_rules')
        max_position_ratio = data.get('max_position_ratio')

        strategy = strategy_manager.strategies[actual_key]

        if new_display_name:
            strategy.name = new_display_name
        
        if description is not None:
            strategy.description = description
        
        if max_position_ratio is not None:
            strategy.max_position_ratio = max(0.0, min(1.0, float(max_position_ratio)))
        
        if rules is not None:
            strategy.rules = []
            from .strategy import StrategyRule
            for rule_data in rules:
                strategy.rules.append(StrategyRule(
                    condition=rule_data.get('condition', ''),
                    action=rule_data.get('action', 'hold'),
                    position_ratio=float(rule_data.get('position_ratio', 1.0)),
                    reason=rule_data.get('reason', ''),
                    connector=rule_data.get('connector', 'OR')
                ))
        
        if exclusion_rules is not None:
            from .strategy import StrategyRule as SR
            strategy.exclusion_rules = []
            for exc_data in exclusion_rules:
                strategy.exclusion_rules.append(SR(
                    condition=exc_data.get('condition', ''),
                    action='hold',
                    position_ratio=0,
                    reason=exc_data.get('reason', ''),
                    connector=exc_data.get('connector', 'OR')
                ))

        market_regime = data.get('market_regime')
        if market_regime is not None:
            strategy.market_regime = market_regime
        
        # 保存到文件
        save_strategies_to_file()
        
        return jsonify({
            'success': True,
            'message': f'策略 {name} 已更新',
            'strategy': strategy.to_dict()
        })
        
    except Exception as e:
        logger.error(f"更新策略失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/strategy/translate', methods=['POST'])
def api_translate_strategy():
    """自然语言和量化规则互相翻译"""
    try:
        data = request.json
        text = data.get('text')
        direction = data.get('direction', 'to_rules')  # to_rules 或 to_natural
        
        if not text:
            return jsonify({'error': '文本不能为空'}), 400
        
        from .strategy import StrategyParser
        parser = StrategyParser()
        
        if direction == 'to_rules':
            # 自然语言转规则
            rules, excl_rules = parser.parse_natural_language(text)
            # 共用操作方向和仓位（取第一条买入规则的值，否则取第一条规则）
            shared_action = next((r.action for r in rules if r.action in ('buy', 'sell')), 'buy')
            shared_ratio  = next((r.position_ratio for r in rules), 0.5)
            result = {
                'shared_action': shared_action,
                'shared_ratio': shared_ratio,
                'rules': [{'condition': r.condition, 'reason': r.reason,
                           'connector': getattr(r, 'connector', 'OR')}
                          for r in rules],
                'exclusion_rules': [{'condition': r.condition, 'reason': r.reason}
                                    for r in excl_rules],
            }
        else:
            # 规则转自然语言
            from .strategy import StrategyRule
            rules = [StrategyRule(**r) for r in text] if isinstance(text, list) else []
            natural_language = parser.translate_to_natural_language(rules)
            result = {'natural_language': natural_language}
        
        return jsonify({
            'success': True,
            'result': result
        })
        
    except Exception as e:
        logger.error(f"翻译策略失败: {e}")
        return jsonify({'error': str(e)}), 500


# ============== 数据更新API ==============

# 全局变量存储更新任务状态
update_task_status = {
    'is_running': False,
    'progress': 0,
    'total': 0,
    'current': '',
    'message': '',
    'last_update': None
}

# 全局实时数据快照（{code_key: info_dict}）和全局开关状态
realtime_snapshot: Dict[str, Any] = {}
global_realtime_state = {'enabled': False}

# ── 评分缓存（每轮实时轮询刷新） ──
_score_cache: Dict[str, dict] = {}
_score_cache_ts: float = 0
_SCORE_CACHE_TTL = 60  # 缓存有效期（秒），后台异步刷新不阻塞请求
_score_refresh_running: bool = False  # 防止并发重复刷新


def _scoring_core(df_ind) -> dict:
    """双轨评分系统 v2.0: 价值体系(V1-V7) + 趋势体系(T1-T7)
    价值体系: 估值极底/多周期超卖/历史价格底部/深度调整/筑底信号/空头钝化/形态安全
    趋势体系: 估值合理/趋势强度/动量强度/RSI健康/对空钝化/趋势形态/量价配合
    最终得分: 0.7×max(value,trend) + 0.3×min(value,trend), 范围[0,100]
    参数: df_ind — 包含技术指标的 DataFrame（至少20行）
    """
    # 前向填充基本面数据（pe_ttm/pb 仅在交易日收盘后更新，最新行可能为 NaN）
    df_ind = df_ind.copy()
    for _fc in ('pe_ttm', 'pb', 'pettm_pct10y'):
        if _fc in df_ind.columns:
            df_ind[_fc] = df_ind[_fc].ffill()

    n = len(df_ind)
    latest = df_ind.iloc[-1]

    def safe(v, default=0.0):
        try:
            f = float(v)
            return f if (f == f) and f != float('inf') and f != float('-inf') else default
        except Exception:
            return default

    def clamp(v, lo=0.0, hi=100.0):
        return max(lo, min(hi, v))

    # ── Basic price data ─────────────────────────────────────────────────────
    close = safe(latest.get('close'))
    rsi6  = safe(latest.get('rsi_6'),  50)
    rsi12 = safe(latest.get('rsi_12'), 50)
    rsi24 = safe(latest.get('rsi_24'), 50)
    ma5   = safe(latest.get('ma_5'),  close)
    ma20  = safe(latest.get('ma_20'), close)
    ma60  = safe(latest.get('ma_60'), close)
    macd_val  = safe(latest.get('macd'))
    macd_sig  = safe(latest.get('macd_signal'))
    macd_hist = safe(latest.get('macd_histogram'))
    boll_upper  = safe(latest.get('boll_upper'),  close * 1.05)
    boll_lower  = safe(latest.get('boll_lower'),  close * 0.95)
    boll_middle = safe(latest.get('boll_middle'), close)
    boll_pos = clamp((close - boll_lower) / (boll_upper - boll_lower), -0.1, 1.1) if boll_upper != boll_lower else 0.5
    kdj_j = safe(latest.get('kdj_j'), 50)
    wr14  = safe(latest.get('wr_14'), -50)

    vol_col = 'volume' if 'volume' in df_ind.columns else 'vol'
    if 'volume_ratio' in df_ind.columns:
        vol_ratio = safe(latest.get('volume_ratio'), 1.0)
    elif vol_col in df_ind.columns and n >= 20:
        recent_v = df_ind[vol_col].iloc[-5:].mean()
        avg_v20  = df_ind[vol_col].iloc[-20:].mean()
        vol_ratio = recent_v / avg_v20 if avg_v20 > 0 else 1.0
    else:
        vol_ratio = 1.0

    pe_raw = safe(latest.get('pe_ttm'), float('nan'))
    pb_raw = safe(latest.get('pb'),     float('nan'))
    pe_avail = (pe_raw == pe_raw)  # not NaN
    pb_avail = (pb_raw == pb_raw)

    def m_pct(days):
        if n > days:
            prev = safe(df_ind.iloc[-(days + 1)].get('close'), close)
            return (close - prev) / prev * 100 if prev > 0 else 0.0
        return 0.0

    m5  = m_pct(5)
    m20 = m_pct(20)
    m60 = m_pct(60)

    # ── 500-day price range position ─────────────────────────────────────────
    lookback = min(n, 500)
    price_window = df_ind['close'].iloc[-lookback:].dropna()
    if len(price_window) > 10:
        h500, l500 = price_window.max(), price_window.min()
        price_pos_pct = clamp((close - l500) / (h500 - l500), 0, 1) if h500 != l500 else 0.5
    else:
        price_pos_pct = 0.5

    # ── Historical percentile helper ─────────────────────────────────────────
    def hist_pct(series, value):
        s = series.dropna()
        if len(s) < 10 or value != value:
            return None
        return (s < value).sum() / len(s)

    pe_hist, pb_hist = None, None
    if pe_avail and pe_raw > 0 and 'pe_ttm' in df_ind.columns:
        pe_s = df_ind['pe_ttm'].iloc[-lookback:]
        pe_s = pe_s[pe_s > 0]
        if len(pe_s) >= 10:
            pe_hist = hist_pct(pe_s, pe_raw)
    if pb_avail and pb_raw > 0 and 'pb' in df_ind.columns:
        pb_s = df_ind['pb'].iloc[-lookback:]
        pb_s = pb_s[pb_s > 0]
        if len(pb_s) >= 10:
            pb_hist = hist_pct(pb_s, pb_raw)

    # ═════════════════════════════════════════════════════════════════════════
    # VALUE SYSTEM (价值体系)
    # ═════════════════════════════════════════════════════════════════════════

    # V1: 估值极底 — weight 15%
    def _pe_val(pct):
        if pct is None: return 50
        if pct < 0.05: return 98
        if pct < 0.10: return 88
        if pct < 0.20: return 75
        if pct < 0.35: return 60
        if pct < 0.50: return 45
        if pct < 0.65: return 28
        if pct < 0.80: return 14
        if pct < 0.90: return 6
        return 2

    def _pb_val(pct, pb_abs):
        base = _pe_val(pct)
        if pb_abs is not None and pb_abs > 0:
            if pb_abs < 1.0: base = min(100, base + 10)
            elif pb_abs < 1.5: base = min(100, base + 5)
        return base

    if pe_avail and pe_raw < 0:
        v1_pe = 10
    elif pe_hist is not None:
        v1_pe = _pe_val(pe_hist)
    else:
        v1_pe = 50
    v1_pb = _pb_val(pb_hist, pb_raw if pb_avail else None)
    if pe_avail and pb_avail:
        V1 = clamp(0.55 * v1_pe + 0.45 * v1_pb)
    elif pe_avail:
        V1 = clamp(float(v1_pe))
    elif pb_avail:
        V1 = clamp(float(v1_pb))
    else:
        V1 = 50.0

    # V2: 多周期RSI超卖 — weight 22%
    def _rsi_val(r):
        if r < 15: return 100
        if r < 20: return 95
        if r < 25: return 88
        if r < 30: return 80
        if r < 35: return 70
        if r < 40: return 57
        if r < 45: return 42
        if r < 50: return 28
        if r < 55: return 15
        if r < 60: return 8
        return 0

    V2 = 0.40 * _rsi_val(rsi6) + 0.35 * _rsi_val(rsi12) + 0.25 * _rsi_val(rsi24)
    if rsi6 < 30 and rsi12 < 35 and rsi24 < 40:
        V2 = min(100, V2 + 12)
    V2 = clamp(V2)

    # V3: 历史价格底部 — weight 15%
    pp = price_pos_pct
    if pp < 0.05:   V3 = 100
    elif pp < 0.10: V3 = 90
    elif pp < 0.20: V3 = 78
    elif pp < 0.30: V3 = 63
    elif pp < 0.40: V3 = 48
    elif pp < 0.50: V3 = 35
    elif pp < 0.65: V3 = 20
    elif pp < 0.80: V3 = 8
    else:           V3 = 0
    V3 = clamp(V3)

    # V4: 深度调整超跌 — weight 12%
    if m60 < -40:   V4 = 100
    elif m60 < -30: V4 = 85
    elif m60 < -20: V4 = 68
    elif m60 < -10: V4 = 45
    elif m60 < -5:  V4 = 25
    elif m60 < 0:   V4 = 10
    else:           V4 = 0
    if m20 < -15: V4 = min(100, V4 + 12)
    elif m20 < -10: V4 = min(100, V4 + 6)
    V4 = clamp(V4)

    # V5: 超跌筑底信号 — weight 15%
    v5_pts = 0
    v5_details = []
    if kdj_j < 0:
        v5_pts += 30; v5_details.append(f'KDJ_J={kdj_j:.1f}极度超卖')
    elif kdj_j < 20:
        v5_pts += 15; v5_details.append(f'KDJ_J={kdj_j:.1f}超卖')
    if wr14 < -90:
        v5_pts += 25; v5_details.append(f'WR={wr14:.1f}深度超卖')
    elif wr14 < -80:
        v5_pts += 15; v5_details.append(f'WR={wr14:.1f}超卖')
    if boll_pos < 0.10:
        v5_pts += 25; v5_details.append('价格近布林下轨')
    elif boll_pos < 0.20:
        v5_pts += 12; v5_details.append('价格接近布林下轨')
    if vol_ratio < 0.7:
        v5_pts += 20; v5_details.append(f'量比={vol_ratio:.2f}缩量探底')
    elif vol_ratio < 0.8:
        v5_pts += 10; v5_details.append(f'量比={vol_ratio:.2f}略缩量')
    if not v5_details:
        v5_details.append('无明显筑底信号')
    V5 = clamp(v5_pts)

    # V6: 空头钝化与稳定 — weight 13%
    v6_pts = 0
    v6_details = []
    if n >= 25 and m20 < 0 and m5 < 0:
        dr5 = m5 / 5
        dr20 = m20 / 20
        if abs(dr5) < abs(dr20) * 0.8:
            v6_pts += 30; v6_details.append('下跌节奏放缓')
    elif m5 >= 0 and m20 < 0:
        v6_pts += 30; v6_details.append('短期止跌回升')
    if n >= 30:
        recent_low_5 = df_ind['close'].iloc[-6:].min()
        prior_low_20 = df_ind['close'].iloc[-26:-6].min()
        if recent_low_5 >= prior_low_20 * 0.99:
            v6_pts += 35; v6_details.append('近期低点守住支撑')
        elif recent_low_5 >= prior_low_20 * 0.96:
            v6_pts += 15; v6_details.append('低点小幅突破')
    if close > boll_lower:
        v6_pts += 20; v6_details.append('价格在布林下轨上方')
    if m5 > 0 and m60 < 0:
        v6_pts += 15; v6_details.append('短期相对长期超额回报')
    if not v6_details:
        v6_details.append('无明显止跌信号')
    V6 = clamp(v6_pts)

    # V7: 形态安全边际 — weight 8%
    v7_details = []
    if n >= 60:
        rl20  = df_ind['close'].iloc[-20:].min()
        pl20  = df_ind['close'].iloc[-40:-20].min()
        pl220 = df_ind['close'].iloc[-60:-40].min()
        if rl20 > pl20 * 1.02 and pl20 > pl220 * 1.02:
            V7 = 92; v7_details.append('连续抬底，反转信号强')
        elif rl20 > pl20 * 1.00:
            V7 = 72; v7_details.append('近期低点抬升，底部稳健')
        elif rl20 > pl20 * 0.97:
            V7 = 50; v7_details.append('低点轻微下移，谨慎')
        elif rl20 > pl20 * 0.92:
            V7 = 22; v7_details.append('低点明显下移，一浪比一浪低')
        else:
            V7 = 0; v7_details.append('连续大幅新低，不宜抄底')
        if pl20 < pl220 and rl20 > pl20 * 1.01:
            V7 = min(100, V7 + 10); v7_details.append('前低后高，潜在底部反转')
    else:
        V7 = 50; v7_details.append('数据不足，默认中性')
    V7 = clamp(V7)

    value_score = clamp(
        0.15 * V1 + 0.22 * V2 + 0.15 * V3 +
        0.12 * V4 + 0.15 * V5 + 0.13 * V6 + 0.08 * V7
    )

    # ═════════════════════════════════════════════════════════════════════════
    # TREND SYSTEM (趋势体系)
    # ═════════════════════════════════════════════════════════════════════════

    # T1: 估值合理未到高位 — weight 12%
    def _pe_trend(pct, pe_abs=None):
        if pe_abs is not None and pe_abs < 0: return 15
        if pct is None: return 60
        if pct > 0.95: return 0
        if pct > 0.85: return 20
        if pct > 0.75: return 40
        if pct > 0.60: return 62
        if pct > 0.40: return 78
        if pct > 0.20: return 88
        return 92

    def _pb_trend(pct, pb_abs):
        base = _pe_trend(pct)
        if pb_abs is not None and pb_abs > 0 and pb_abs < 1.0:
            base = min(100, base + 5)
        return base

    if pe_avail and pe_raw < 0:
        t1_pe = 15
    elif pe_hist is not None:
        t1_pe = _pe_trend(pe_hist, pe_raw)
    else:
        t1_pe = 60
    t1_pb = _pb_trend(pb_hist, pb_raw if pb_avail else None)
    if pe_avail and pb_avail:
        T1 = clamp(0.55 * t1_pe + 0.45 * t1_pb)
    elif pe_avail:
        T1 = clamp(float(t1_pe))
    elif pb_avail:
        T1 = clamp(float(t1_pb))
    else:
        T1 = 60.0

    # T2: 趋势强度 — weight 22%
    t2_details = []
    if close > ma5 and ma5 > ma20 and ma20 > ma60:
        ma_score = 90; t2_details.append('多头完全排列')
    elif close > ma20 and ma20 > ma60:
        ma_score = 75; t2_details.append('中长期多头')
    elif close > ma60:
        ma_score = 55; t2_details.append('站上MA60')
    elif close > ma20:
        ma_score = 35; t2_details.append('短期多头，未站MA60')
    elif close > ma5:
        ma_score = 20; t2_details.append('仅短期多头')
    else:
        ma_score = 5;  t2_details.append('空头排列')
    macd_bonus = 0
    if macd_val > 0 and macd_val > macd_sig:
        macd_bonus = 20; t2_details.append('MACD零轴上方金叉')
    elif macd_hist > 0:
        macd_bonus = 10; t2_details.append('MACD柱上翻')
    elif macd_val > macd_sig:
        macd_bonus = 5;  t2_details.append('MACD金叉(零轴下)')
    T2 = clamp(ma_score + macd_bonus)

    # T3: 动量强度 — weight 18%
    def _m5t(m):
        if m > 20:  return 55
        if m > 10:  return 88
        if m > 3:   return 100
        if m > 0:   return 72
        if m > -5:  return 42
        if m > -10: return 18
        return 0

    def _m20t(m):
        if m > 35:  return 55
        if m > 20:  return 90
        if m > 8:   return 100
        if m > 3:   return 80
        if m > 0:   return 60
        if m > -5:  return 38
        if m > -10: return 18
        return 5

    def _m60t(m):
        if m > 60:  return 50
        if m > 35:  return 85
        if m > 15:  return 100
        if m > 5:   return 88
        if m > 0:   return 68
        return 10

    T3 = clamp(0.40 * _m5t(m5) + 0.35 * _m20t(m20) + 0.25 * _m60t(m60))

    # T4: RSI技术健康区间 — weight 12%
    def _rsi_trend(r):
        if 45 <= r <= 62:  return 100
        if 40 <= r < 45:   return 85
        if 62 < r <= 70:   return 78
        if 35 <= r < 40:   return 68
        if 70 < r <= 78:   return 50
        if 25 <= r < 35:   return 45
        if 78 < r <= 85:   return 22
        if r > 85:         return 8
        return 30  # < 25

    T4 = clamp(0.55 * _rsi_trend(rsi6) + 0.45 * _rsi_trend(rsi12))

    # T5: 对空钝化/积累信号 — weight 13%
    t5_pts = 0
    t5_details = []
    if vol_col in df_ind.columns and n >= 20:
        recent20 = df_ind.iloc[-20:].copy()
        prev_close_s = recent20['close'].shift(1)
        up_mask   = recent20['close'] >= prev_close_s
        down_mask = ~up_mask
        up_vols   = recent20.loc[up_mask,   vol_col]
        down_vols = recent20.loc[down_mask, vol_col]
        if len(up_vols) > 0 and len(down_vols) > 0:
            avg_up = up_vols.mean()
            avg_dn = down_vols.mean()
            if avg_up > avg_dn * 1.3:
                t5_pts += 35; t5_details.append('上涨日量能远大于下跌日')
            elif avg_up > avg_dn * 1.1:
                t5_pts += 20; t5_details.append('上涨日量能大于下跌日')
            elif avg_up > avg_dn * 0.9:
                t5_pts += 10; t5_details.append('量能均衡')
    if boll_pos > 0.5:
        t5_pts += 25; t5_details.append('价格站在布林中轨上方')
    elif boll_pos > 0.35:
        t5_pts += 10; t5_details.append('价格近布林中轨')
    if n >= 25:
        dr5_t  = m5  / 5
        dr20_t = m20 / 20
        if dr5_t > dr20_t * 0.5:
            t5_pts += 20; t5_details.append('短期日均涨幅强于中期')
    if close > ma5 and close > ma20:
        t5_pts += 20; t5_details.append('价格站稳均线上方')
    elif close > ma20:
        t5_pts += 10
    if not t5_details:
        t5_details.append('无明显积累信号')
    T5 = clamp(t5_pts)

    # T6: 趋势形态 — weight 13%
    t6_details = []
    if n >= 60:
        rh20 = df_ind['close'].iloc[-20:].max()
        ph20 = df_ind['close'].iloc[-40:-20].max()
        rl20_t = df_ind['close'].iloc[-20:].min()
        pl20_t = df_ind['close'].iloc[-40:-20].min()
        hh = rh20 > ph20 * 1.01
        hl = rl20_t > pl20_t * 1.01
        if hh and hl:
            t6_base = 100; t6_details.append('一浪比一浪高，高低点均抬升')
        elif hh:
            t6_base = 68;  t6_details.append('高点抬升，低点未抬升')
        elif hl:
            t6_base = 62;  t6_details.append('低点抬升，高点未突破')
        elif rh20 >= ph20 * 0.99 and rl20_t >= pl20_t * 0.99:
            t6_base = 42;  t6_details.append('震荡整理')
        else:
            t6_base = 10;  t6_details.append('高低点均下移，趋势不佳')
        fast_rise_bonus = 0
        if m60 > 15 and m5 < 0 and abs(m5) < m60 / 12:
            fast_rise_bonus = 20; t6_details.append('快速上涨后小幅回调，趋势健康')
        elif m20 > 8 and m5 < 0 and abs(m5) < m20 / 4:
            fast_rise_bonus = 12; t6_details.append('中期上涨后小幅回调')
        T6 = clamp(t6_base + fast_rise_bonus)
    else:
        T6 = 50; t6_details.append('数据不足，默认中性')

    # T7: 量价配合 — weight 10%
    t7_details = []
    if vol_ratio > 3:
        if m5 > 0: T7 = 55; t7_details.append('异常放量上涨，注意阶段高点')
        else:       T7 = 15; t7_details.append('异常放量下跌，出货风险')
    elif vol_ratio > 1.5:
        if m5 > 0: T7 = 100; t7_details.append('放量上涨，趋势确认')
        else:       T7 = 25;  t7_details.append('放量下跌，趋势破坏')
    elif vol_ratio > 1.1:
        if m5 > 0: T7 = 82; t7_details.append('温和放量上涨')
        else:       T7 = 40; t7_details.append('轻度放量下跌')
    elif vol_ratio > 0.7:
        if m5 > 0: T7 = 65; t7_details.append('缩量上涨，待量能确认')
        else:       T7 = 72; t7_details.append('缩量回调，健康整理')
    else:
        if m5 > 0: T7 = 55; t7_details.append('极度缩量上涨')
        else:       T7 = 78; t7_details.append('极度缩量回调，惜售信号')
    T7 = clamp(T7)

    trend_score_total = clamp(
        0.12 * T1 + 0.22 * T2 + 0.18 * T3 +
        0.12 * T4 + 0.13 * T5 + 0.13 * T6 + 0.10 * T7
    )

    # ── Final Score ───────────────────────────────────────────────────────────
    total_score = round(clamp(
        0.7 * max(value_score, trend_score_total) +
        0.3 * min(value_score, trend_score_total)
    ), 2)

    if trend_score_total > value_score + 5:
        dominant = '趋势主导'
    elif value_score > trend_score_total + 5:
        dominant = '价值主导'
    else:
        dominant = '趋势+价值均衡'

    rating, rating_color = score_classification(total_score)

    return {
        'close': close,
        'total_score': total_score,
        'rating': rating,
        'rating_color': rating_color,
        'dominant': dominant,
        # dual scores (popular_score kept for JS backward-compat = trend)
        'popular_score': round(trend_score_total, 1),
        'value_score':   round(value_score, 1),
        'trend_score_total': round(trend_score_total, 1),
        # value sub-scores
        'V1': round(V1, 1), 'V2': round(V2, 1), 'V3': round(V3, 1),
        'V4': round(V4, 1), 'V5': round(V5, 1), 'V6': round(V6, 1), 'V7': round(V7, 1),
        # trend sub-scores
        'T1': round(T1, 1), 'T2': round(T2, 1), 'T3': round(T3, 1),
        'T4': round(T4, 1), 'T5': round(T5, 1), 'T6': round(T6, 1), 'T7': round(T7, 1),
        # raw factors
        'rsi6': round(rsi6, 2), 'rsi12': round(rsi12, 2), 'rsi24': round(rsi24, 2),
        'boll_pos': round(boll_pos, 3),
        'm5': round(m5, 2), 'm20': round(m20, 2), 'm60': round(m60, 2),
        'vol_ratio': round(vol_ratio, 2),
        'macd_val': round(macd_val, 4), 'macd_sig': round(macd_sig, 4),
        'price_pos_pct': round(price_pos_pct * 100, 1),
        'kdj_j': round(kdj_j, 1), 'wr14': round(wr14, 1),
        'pe_ttm': pe_raw if pe_avail else None,
        'pb': pb_raw if pb_avail else None,
        'pe_hist_pct': round(pe_hist * 100, 1) if pe_hist is not None else None,
        'pb_hist_pct': round(pb_hist * 100, 1) if pb_hist is not None else None,
        # details
        't2_details': t2_details, 't5_details': t5_details,
        't6_details': t6_details, 't7_details': t7_details,
        'v5_details': v5_details, 'v6_details': v6_details, 'v7_details': v7_details,
    }


def compute_stock_score(code: str) -> dict:
    """快速计算单只股票的综合评分，返回 {total_score, rating, rating_color, dominant, dimensions}"""
    default_result = {'total_score': None, 'rating': '-', 'rating_color': '#999'}
    try:
        df = unified_data.get_historical_data(code)
        if df is None or df.empty:
            return default_result

        if 'trade_date' in df.columns and 'date' not in df.columns:
            df = df.rename(columns={'trade_date': 'date'})
        if 'vol' in df.columns and 'volume' not in df.columns:
            df = df.rename(columns={'vol': 'volume'})
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df_ind = technical_indicators.calculate_all_indicators_from_df(df)
        if df_ind.empty or len(df_ind) < 20:
            return default_result

        s = _scoring_core(df_ind)
        return {
            'total_score': s['total_score'],
            'rating': s['rating'],
            'rating_color': s['rating_color'],
            'dominant': s['dominant'],
            'dimensions': {
                'value_score': s['value_score'],
                'trend_score': s['trend_score_total'],
                'V1_valuation': s['V1'],
                'V2_rsi_oversold': s['V2'],
                'T2_trend_strength': s['T2'],
                'T3_momentum': s['T3'],
            },
        }
    except Exception as e:
        logger.error(f"compute_stock_score({code}) error: {e}")
        return default_result

def score_classification(total_score):
    if total_score >= 80:
        rating, rating_color = '顶尖', '#4caf50'       # 绿色
    elif total_score >= 75:
        rating, rating_color = '优秀', '#8bc34a'       # 浅绿
    elif total_score >= 65:
        rating, rating_color = '良好', '#8bc34a'       # 浅绿
    elif total_score >= 50:
        rating, rating_color = '及格', '#ff9800'       # 橙色
    else:
        rating, rating_color = '不及格', '#f44336'     # 红色
    return rating, rating_color


def get_all_scores() -> Dict[str, dict]:
    """批量获取所有监控股票评分，带缓存。
    缓存过期时在后台线程异步刷新，主线程立即返回旧缓存（或空），绝不阻塞请求。
    """
    import time, threading
    global _score_cache, _score_cache_ts, _score_refresh_running

    now = time.time()
    if now - _score_cache_ts < _SCORE_CACHE_TTL and _score_cache:
        return _score_cache

    # 缓存过期：异步刷新，当前请求立即返回旧缓存
    if not _score_refresh_running:
        _score_refresh_running = True

        def _do_refresh():
            global _score_cache, _score_cache_ts, _score_refresh_running
            try:
                scores = {}
                for stock in stock_manager.get_all_stocks():
                    try:
                        code = stock.full_code if hasattr(stock, 'full_code') else stock.code
                        scores[code] = compute_stock_score(code)
                    except Exception:
                        pass
                _score_cache = scores
                _score_cache_ts = time.time()
            finally:
                _score_refresh_running = False

        t = threading.Thread(target=_do_refresh, daemon=True)
        t.start()

    return _score_cache  # 立即返回（可能为空或过期）

@app.route('/api/data/update', methods=['POST'])
def api_data_update():
    """一键更新数据"""
    global update_task_status
    
    try:
        data = request.json or {}
        codes = data.get('codes')  # 如果指定了代码列表，只更新这些
        update_type = data.get('type', 'all')  # all, history, news, indicators
        
        # 检查是否已有任务在运行
        if update_task_status['is_running']:
            return jsonify({'error': '已有更新任务在运行', 'status': update_task_status}), 429
        
        # 在后台线程中执行更新
        import threading
        def do_update():
            global update_task_status
            update_task_status['is_running'] = True
            update_task_status['progress'] = 0
            update_task_status['message'] = '开始更新...'
            
            try:
                stocks = stock_manager.get_all_stocks()
                if codes:
                    stocks = [s for s in stocks if s.code in codes]
                
                update_task_status['total'] = len(stocks)
                
                for i, stock in enumerate(stocks):
                    update_task_status['current'] = stock.full_code
                    update_task_status['progress'] = int((i / len(stocks)) * 100)
                    
                    try:
                        # 更新历史数据
                        if update_type in ['all', 'history']:
                            update_task_status['message'] = f'更新 {stock.name} 历史数据...'
                            unified_data.get_historical_data(stock.full_code)
                        
                        # 更新新闻
                        if update_type in ['all', 'news']:
                            update_task_status['message'] = f'采集 {stock.name} 新闻...'
                            news_df = news_collector.fetch_stock_news(stock.full_code, max_pages=3)
                            if not news_df.empty:
                                sentiment_analyzer.analyze_news_df(news_df)
                        
                        # 更新技术指标
                        if update_type in ['all', 'indicators']:
                            update_task_status['message'] = f'计算 {stock.name} 技术指标...'
                            df = technical_indicators.calculate_all_indicators(stock.full_code)
                            if not df.empty:
                                technical_indicators.save_indicators(stock.full_code, df)
                        
                    except Exception as e:
                        logger.error(f"更新 {stock.full_code} 失败: {e}")
                        continue
                
                update_task_status['progress'] = 100
                update_task_status['message'] = '更新完成'
                update_task_status['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
            finally:
                update_task_status['is_running'] = False
        
        thread = threading.Thread(target=do_update)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': '更新任务已启动',
            'status': update_task_status
        })
        
    except Exception as e:
        logger.error(f"启动更新任务失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/data/update/status')
def api_data_update_status():
    """获取数据更新状态"""
    return jsonify(update_task_status)


@app.route('/api/data/realtime', methods=['POST'])
def api_data_realtime():
    """启动实时数据更新"""
    global update_task_status
    
    try:
        data = request.json or {}
        codes = data.get('codes')
        interval = data.get('interval', 5)  # 默认5秒
        duration = data.get('duration', 300)  # 默认5分钟
        
        if update_task_status['is_running']:
            return jsonify({'error': '已有任务在运行'}), 429
        
        import threading
        import time
        
        def do_realtime_update(is_global=False):
            global update_task_status, realtime_snapshot
            update_task_status['is_running'] = True
            update_task_status['message'] = '实时更新已启动'
            
            start_time = time.time()
            update_count = 0
            
            try:
                while update_task_status['is_running'] and (time.time() - start_time < duration):
                    # 如果是全局模式，当全局开关关闭时退出
                    if is_global and not global_realtime_state['enabled']:
                        break

                    update_count += 1
                    update_task_status['message'] = f'第 {update_count} 次实时更新...'
                    try:
                        # 准备统一后缀格式代码（如 '000001.SH', '09988.HK'）
                        unified_codes = None
                        if codes:
                            unified_codes = []
                            for c in codes:
                                stock = stock_manager.get_stock_by_code(c)
                                if stock:
                                    unified_codes.append(stock.full_code)
                                else:
                                    unified_codes.append(str(c))
                            if not unified_codes:
                                unified_codes = None

                        # 调用 data_sourcing 获取实时数据（None 表示获取全部监控股票）
                        if unified_codes is not None:
                            realtime_data_df = unified_data.get_realtime_data(unified_codes, adjust=True)
                        else:
                            realtime_data_df = unified_data.get_realtime_data(adjust=True)
                        realtime_data = {row.get('code', ''): row for _, row in realtime_data_df.iterrows()} if not realtime_data_df.empty else {}

                    except Exception as e:
                        logger.error(f"获取实时数据失败: {e}")
                        realtime_data = {}

                    # 将实时数据缓存到快照，并合并到历史 CSV
                    try:
                        if realtime_data:
                            realtime_snapshot.update(realtime_data)
                            unified_data.merge_realtime_data(realtime_data)
                            update_task_status['message'] = f'第 {update_count} 次实时数据已合并'
                        else:
                            update_task_status['message'] = f'第 {update_count} 次未获取到实时数据'
                    except Exception as e:
                        logger.error(f"合并实时数据失败: {e}")

                    update_task_status['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    time.sleep(interval)
                
                update_task_status['message'] = '实时更新已结束'
                
            finally:
                update_task_status['is_running'] = False
                if is_global:
                    global_realtime_state['enabled'] = False
        
        thread = threading.Thread(target=do_realtime_update, kwargs={'is_global': False})
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'实时更新已启动，将持续 {duration} 秒'
        })
        
    except Exception as e:
        logger.error(f"启动实时更新失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/data/realtime/stop', methods=['POST'])
def api_data_realtime_stop():
    """停止实时数据更新"""
    global update_task_status, global_realtime_state
    update_task_status['is_running'] = False
    global_realtime_state['enabled'] = False
    update_task_status['message'] = '已停止'
    return jsonify({'success': True, 'message': '实时更新已停止'})


@app.route('/api/data/realtime/global', methods=['GET', 'POST'])
def api_data_realtime_global():
    """全局实时更新开关 GET 返回当前状态，POST {enabled: bool} 开启/关闭"""
    global update_task_status, global_realtime_state

    if request.method == 'GET':
        return jsonify({
            'enabled': global_realtime_state['enabled'],
            'is_running': update_task_status['is_running'],
            'last_update': update_task_status.get('last_update'),
            'message': update_task_status.get('message', ''),
        })

    data = request.json or {}
    enabled = bool(data.get('enabled', False))
    interval = int(data.get('interval', 5))

    if enabled:
        global_realtime_state['enabled'] = True
        if not update_task_status['is_running']:
            import threading, time

            def _global_update_loop():
                global update_task_status, realtime_snapshot, global_realtime_state
                update_task_status['is_running'] = True
                update_task_status['message'] = '全局实时更新已启动'
                update_count = 0

                def _do_fetch():
                    """执行一次全量实时数据拉取并合并"""
                    nonlocal update_count
                    update_count += 1
                    update_task_status['message'] = f'全局第 {update_count} 次实时更新...'
                    try:
                        realtime_data_df = unified_data.get_realtime_data(adjust=True)
                        rt = {row.get('code', ''): row for _, row in realtime_data_df.iterrows()} if not realtime_data_df.empty else {}
                    except Exception as e:
                        logger.error(f"全局实时数据获取失败: {e}")
                        rt = {}
                    try:
                        if rt:
                            realtime_snapshot.update(rt)
                            unified_data.merge_realtime_data(rt)
                            update_task_status['message'] = f'全局第 {update_count} 次已合并 ({len(rt)} 只)'
                    except Exception as e:
                        logger.error(f"全局合并失败: {e}")
                    update_task_status['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                try:
                    # 立即做一次全量更新（无论是否在交易时间）
                    _do_fetch()
                    # 若非交易时间，完成初始更新后停止
                    if not is_trading_time():
                        update_task_status['message'] = '已完成全量更新，当前非交易时间，实时更新已停止'
                        return
                    # 交易时间：持续循环更新
                    while update_task_status['is_running'] and global_realtime_state['enabled']:
                        time.sleep(interval)
                        if not is_trading_time():
                            update_task_status['message'] = '已离开交易时间，实时更新已停止'
                            break
                        _do_fetch()
                    update_task_status['message'] = '全局实时更新已结束'
                finally:
                    update_task_status['is_running'] = False
                    global_realtime_state['enabled'] = False

            t = threading.Thread(target=_global_update_loop, daemon=True)
            t.start()
        return jsonify({'success': True, 'enabled': True})
    else:
        global_realtime_state['enabled'] = False
        update_task_status['is_running'] = False
        update_task_status['message'] = '全局实时更新已停止'
        return jsonify({'success': True, 'enabled': False})


@app.route('/api/data/realtime/latest')
def api_data_realtime_latest():
    """返回最新实时数据快照（供前端轮询）"""
    import math

    def _sanitize(val):
        """将 NaN/Inf 转为 None，避免 JSON 序列化异常"""
        if val is None:
            return None
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return None
        return val

    result = {}
    # 构建 full_code → stock 字典，将循环内的 O(N) 线性扫描降为 O(1)
    stock_lookup = {s.full_code: s for s in stock_manager.get_all_stocks()}

    for code_key, info in realtime_snapshot.items():
        stock = stock_lookup.get(code_key)
        if stock is None:
            # 股票已从监控列表移除，跳过（同时清理快照）
            continue

        current_price = _sanitize(info.get('now', info.get('price')))
        prev_close = _sanitize(info.get('lastPrice', info.get('prev_close')))
        change_val = _sanitize(info.get('change'))
        pct_val = _sanitize(info.get('pct_chg', info.get('dtd')))

        # 如果缺少涨跌额/涨跌幅，从当前价和昨收计算
        if change_val is None and current_price and prev_close:
            try:
                cp = float(current_price)
                pc = float(prev_close)
                if pc > 0:
                    change_val = round(cp - pc, 4)
                    if pct_val is None:
                        pct_val = round((cp - pc) / pc * 100, 2)
            except (ValueError, TypeError):
                pass

        result[code_key] = {
            'name': stock.name if stock else code_key,
            'code': stock.code if stock else code_key,
            'full_code': stock.full_code if stock else code_key,
            'market': stock.market if stock else '',
            'price': current_price,
            'open': _sanitize(info.get('open', info.get('openPrice'))),
            'high': _sanitize(info.get('high')),
            'low': _sanitize(info.get('low')),
            'change': change_val,
            'pct_chg': pct_val,
            'volume': _sanitize(info.get('volume')),
            'amount': _sanitize(info.get('amount')),
            'notes': stock.notes if stock else '',
            'data_time': info.get('data_time', ''),   # Sina-reported time HH:MM:SS
            'data_date': info.get('data_date', ''),   # Sina-reported date YYYY-MM-DD
        }

    # 批量附加评分
    try:
        all_scores = get_all_scores()
        for code_key, data in result.items():
            full_code = data.get('full_code', code_key)
            sc = all_scores.get(full_code) or all_scores.get(code_key) or {}
            data['score'] = sc.get('total_score')
            data['rating'] = sc.get('rating', '-')
            data['rating_color'] = sc.get('rating_color', '#999')
            data['dominant'] = sc.get('dominant', '')
    except Exception as e:
        logger.error(f"批量评分附加失败: {e}")

    # Store intraday snapshot — 仅在交易时间内采集
    now_str = datetime.now().strftime('%H:%M')
    for code_key, data in result.items():
        # 使用 full_code 作为 key，避免同代码不同市场冲突
        full_code = data.get('full_code', code_key)
        # 根据股票市场判断是否处于交易时间
        stock_market = data.get('market', '')
        if not is_trading_time(stock_market if stock_market else None):
            continue
        price = data.get('price')
        if price:
            if full_code not in INTRADAY_SNAPSHOTS:
                INTRADAY_SNAPSHOTS[full_code] = []
            snaps = INTRADAY_SNAPSHOTS[full_code]
            if not snaps or snaps[-1].get('time') != now_str:
                avg_price = sum(s['price'] for s in snaps if s.get('price')) / len(snaps) if snaps else float(price)
                snaps.append({
                    'time': now_str,
                    'price': float(price),
                    'avg_price': round(avg_price, 2),
                    'volume': float(data.get('volume', 0) or 0),
                })
                INTRADAY_SNAPSHOTS[full_code] = snaps[-240:]

    return jsonify({
        'data': result,
        'is_running': update_task_status.get('is_running', False),
        'global_enabled': global_realtime_state['enabled'],
        'last_update': update_task_status.get('last_update'),
    })


@app.route('/api/data/export')
def api_data_export():
    """Export stock data as CSV"""
    from flask import Response
    import io
    try:
        code = request.args.get('code')
        data_type = request.args.get('type', 'daily')
        start_date = request.args.get('start', '')
        end_date = request.args.get('end', '')

        if not code:
            return jsonify({'error': 'code required'}), 400

        if data_type == 'daily':
            df = unified_data.get_historical_data(code)
        elif data_type == 'indicators':
            from .indicators import technical_indicators
            df_hist = unified_data.get_historical_data(code)
            df = technical_indicators.calculate_all_indicators_from_df(df_hist) if df_hist is not None and not df_hist.empty else pd.DataFrame()
        else:
            df = unified_data.get_historical_data(code)

        if df is None or df.empty:
            return jsonify({'error': 'No data found'}), 404

        # Filter by date range
        if 'date' in df.columns and (start_date or end_date):
            df['date'] = pd.to_datetime(df['date'])
            if start_date:
                df = df[df['date'] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df['date'] <= pd.to_datetime(end_date)]

        csv_content = df.to_csv(index=False, encoding='utf-8-sig')
        stock = stock_manager.get_stock_by_code(code)
        filename = f"{code}_{stock.name if stock else code}_{data_type}.csv"

        return Response(
            csv_content,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        logger.error(f"api_data_export error: {e}")
        return jsonify({'error': str(e)}), 500



@app.route('/api/scheduler/status')
def api_scheduler_status():
    """获取调度器状态"""
    try:
        status = scheduler.get_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"获取调度器状态失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/start', methods=['POST'])
def api_scheduler_start():
    """启动调度器"""
    try:
        if scheduler.is_running:
            return jsonify({'success': True, 'message': '调度器已在运行'})
        scheduler.start()
        return jsonify({'success': True, 'message': '调度器已启动'})
    except Exception as e:
        logger.error(f"启动调度器失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/stop', methods=['POST'])
def api_scheduler_stop():
    """停止调度器"""
    try:
        if not scheduler.is_running:
            return jsonify({'success': True, 'message': '调度器未运行'})
        scheduler.stop()
        return jsonify({'success': True, 'message': '调度器已停止'})
    except Exception as e:
        logger.error(f"停止调度器失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/config', methods=['GET', 'POST'])
def api_scheduler_config():
    """获取或更新调度器配置"""
    try:
        if request.method == 'GET':
            return jsonify(scheduler.config)
        
        elif request.method == 'POST':
            data = request.json
            scheduler.update_config(data)
            return jsonify({
                'success': True,
                'config': scheduler.config
            })
    except Exception as e:
        logger.error(f"操作调度器配置失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/run', methods=['POST'])
def api_scheduler_run():
    """手动触发执行所有自定义任务"""
    try:
        import threading
        def _run_all():
            tasks = scheduler.get_custom_tasks()
            for t in tasks:
                if t.get('enabled', True):
                    try:
                        scheduler.run_custom_task(t['id'], manual=True)
                    except Exception as e:
                        logger.error(f"手动全部执行 - 任务 {t['id']} 失败: {e}")
        thread = threading.Thread(target=_run_all, daemon=True)
        thread.start()
        return jsonify({
            'success': True,
            'message': '全部任务已在后台启动'
        })
    except Exception as e:
        logger.error(f"启动任务失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/run/<task_name>', methods=['POST'])
def api_scheduler_run_task(task_name):
    """手动触发单个任务"""
    try:
        result = scheduler.run_single_task(task_name)
        if result.get('success'):
            return jsonify(result)
        return jsonify(result), 400
    except Exception as e:
        logger.error(f"手动触发任务失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/tasks')
def api_scheduler_tasks():
    """获取所有任务的执行状态"""
    try:
        return jsonify(scheduler.get_task_status())
    except Exception as e:
        logger.error(f"获取任务状态失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/custom_tasks', methods=['GET', 'POST'])
def api_scheduler_custom_tasks():
    """管理自定义任务（列表 / 新增）"""
    try:
        if request.method == 'GET':
            tasks = scheduler.get_custom_tasks()
            return jsonify(tasks)
        data = request.json or {}
        time_str = data.get('time')
        name = data.get('name')
        content = data.get('content', '')
        types = data.get('types')
        enabled = bool(data.get('enabled', True))
        skip = bool(data.get('skip_non_trading_day', True))
        if not time_str or not name or not types:
            return jsonify({'error': 'time, name and types required'}), 400
        if not isinstance(types, list):
            return jsonify({'error': 'types must be a list'}), 400
        task = scheduler.add_custom_task(time_str, content, name=name, types=types, enabled=enabled, skip_non_trading_day=skip)
        return jsonify({'success': True, 'task': task})
    except Exception as e:
        logger.error(f"自定义任务操作失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/custom_tasks/<task_id>', methods=['PUT', 'DELETE'])
def api_scheduler_custom_task(task_id):
    """更新或删除单个自定义任务"""
    try:
        if request.method == 'PUT':
            data = request.json or {}
            time = data.get('time')
            content = data.get('content')
            enabled = data.get('enabled') if 'enabled' in data else None
            name = data.get('name') if 'name' in data else None
            types = data.get('types') if 'types' in data else None
            skip = data.get('skip_non_trading_day') if 'skip_non_trading_day' in data else None
            ok = scheduler.update_custom_task(task_id, time_str=time, content=content, enabled=enabled, name=name, types=types, skip_non_trading_day=skip)
            if ok:
                return jsonify({'success': True})
            return jsonify({'error': 'task not found'}), 404
        else:
            # 支持客户端传入带 custom_ 前缀的 id
            actual_id = task_id
            if isinstance(actual_id, str) and actual_id.startswith('custom_'):
                actual_id = actual_id.replace('custom_', '', 1)

            ok = scheduler.remove_custom_task(actual_id)
            if ok:
                return jsonify({'success': True})

            # 回退措施：直接从配置里移除（兼容不一致的运行时状态）
            try:
                tasks = scheduler.config.get('custom_tasks', [])
                removed = False
                for t in list(tasks):
                    if t.get('id') == actual_id or t.get('id') == task_id:
                        # 取消调度（如果有的话）
                        job_id = f"custom_task_{t.get('id')}"
                        try:
                            if getattr(scheduler, 'scheduler', None):
                                scheduler.scheduler.remove_job(job_id)
                        except Exception:
                            pass
                        try:
                            tasks.remove(t)
                        except ValueError:
                            pass
                        scheduler._save_config()
                        removed = True
                        break
                if removed:
                    return jsonify({'success': True})
            except Exception as e:
                logger.error(f"删除自定义任务回退失败: {e}")

            return jsonify({'error': 'task not found'}), 404
    except Exception as e:
        logger.error(f"自定义任务单项操作失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/custom_tasks/<task_id>/run', methods=['POST'])
def api_run_custom_task(task_id):
    """立即执行自定义任务（手动触发，跳过非交易日检查）"""
    try:
        import threading
        def _worker():
            try:
                scheduler.run_custom_task(task_id, manual=True)
            except Exception as e:
                logger.error(f"手动执行自定义任务失败: {e}")
        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        return jsonify({'success': True, 'message': '任务已在后台启动'})
    except Exception as e:
        logger.error(f"执行自定义任务失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/task/<task_name>', methods=['PUT', 'DELETE'])
def api_scheduler_task(task_name):
    """启用/禁用或删除内建任务"""
    try:
        if request.method == 'PUT':
            data = request.json or {}
            # 支持更新 enabled 或 skip_non_trading_day 任一项
            if 'enabled' not in data and 'skip_non_trading_day' not in data:
                return jsonify({'error': 'enabled or skip_non_trading_day required'}), 400
            cfg = scheduler.config.setdefault('task_schedule', {}).setdefault(task_name, {})
            if 'enabled' in data:
                cfg['enabled'] = bool(data['enabled'])
            if 'skip_non_trading_day' in data:
                cfg['skip_non_trading_day'] = bool(data['skip_non_trading_day'])
            scheduler._save_config()
            # 如果正在运行，尝试移除旧 job 并重启以应用新配置
            if scheduler.is_running:
                try:
                    job_id = f"task_{task_name}"
                    if getattr(scheduler, 'scheduler', None):
                        try:
                            scheduler.scheduler.remove_job(job_id)
                        except Exception:
                            pass
                    scheduler.stop()
                    scheduler.start()
                except Exception as e:
                    logger.exception(f"重启调度器失败: {e}")
            return jsonify({'success': True})
        else:
            # DELETE：尽量幂等处理——即使配置中不存在也返回成功
            try:
                job_id = f"task_{task_name}"
                if getattr(scheduler, 'scheduler', None):
                    try:
                        scheduler.scheduler.remove_job(job_id)
                    except Exception:
                        pass
            except Exception:
                pass

            # 从配置中移除（如果存在）
            if task_name in scheduler.config.get('task_schedule', {}):
                scheduler.config['task_schedule'].pop(task_name, None)
                scheduler._save_config()

            # 如果正在运行，重启以应用变更
            if scheduler.is_running:
                try:
                    scheduler.stop()
                    scheduler.start()
                except Exception as e:
                    logger.exception(f"删除任务后重启调度器失败: {e}")
            return jsonify({'success': True})
    except Exception as e:
        logger.error(f"任务操作失败: {e}")
        return jsonify({'error': str(e)}), 500


# ============== 分时/盘口数据API ==============

@app.route('/api/stock/<code>/intraday')
def api_stock_intraday(code):
    """Get intraday (分时) data for a stock"""
    try:
        # 统一转为后缀格式来查找
        stock = stock_manager.get_stock_by_code(code)
        lookup_code = stock.full_code if stock else code
        
        snapshots = INTRADAY_SNAPSHOTS.get(code, []) or INTRADAY_SNAPSHOTS.get(lookup_code, [])
        
        # Also get current realtime data
        realtime_data = {}
        if lookup_code in realtime_snapshot:
            info = realtime_snapshot[lookup_code]
            realtime_data = {
                'price': info.get('now', info.get('price')),
                'open': info.get('open'),
                'high': info.get('high'),
                'low': info.get('low'),
                'volume': info.get('volume'),
                'pct_chg': info.get('pct_chg'),
            }
        else:
            try:
                rt = unified_data.get_realtime_data([code]) if hasattr(unified_data, 'get_realtime_data') else None
                if rt and code in rt:
                    realtime_data = rt[code]
            except Exception:
                pass
        
        # Build response
        resp = {
            'success': True,
            'code': code,
            'snapshots': snapshots[-240:],  # Last 240 minutes (full trading day)
            'intraday_available': len(snapshots) > 0,
        }
        if realtime_data:
            resp.update({
                'current_price': float(realtime_data.get('price', realtime_data.get('now', 0)) or 0),
                'open': float(realtime_data.get('open', 0) or 0),
                'high': float(realtime_data.get('high', 0) or 0),
                'low': float(realtime_data.get('low', 0) or 0),
                'volume': float(realtime_data.get('volume', 0) or 0),
                'change_pct': float(realtime_data.get('pct_chg', realtime_data.get('change_pct', 0)) or 0),
            })
        
        if not resp.get('current_price') and not snapshots:
            resp['message'] = '暂无分时数据，请开启实时更新以采集分时数据'
        
        return jsonify(resp)
    except Exception as e:
        logger.error(f"api_stock_intraday error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/stock/<code>/chip_distribution')
def api_chip_distribution(code):
    """获取筹码分布数据（基于近期价格/成交量统计）"""
    try:
        df = unified_data.get_historical_data(code)
        if df is None or df.empty:
            return jsonify({'success': False, 'error': 'No data'})

        df = df.copy()
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['close', 'volume'])

        # Simple chip distribution: price histogram weighted by volume (recent 250 bars)
        last_n = min(250, len(df))
        df_recent = df.tail(last_n).reset_index(drop=True)
        price_min = float(df_recent['low'].min())
        price_max = float(df_recent['high'].max())
        n_bins = 50
        bin_size = (price_max - price_min) / n_bins if price_max > price_min else 1.0

        chip_data = []
        total_bars = len(df_recent)
        for i in range(n_bins):
            bin_low = price_min + i * bin_size
            bin_high = bin_low + bin_size
            bin_mid = round((bin_low + bin_high) / 2, 2)
            # Weight recent bars more heavily
            weighted_vol = 0.0
            for idx, row in df_recent.iterrows():
                if row['close'] >= bin_low and row['close'] < bin_high:
                    recency_weight = (idx + 1) / total_bars
                    weighted_vol += float(row['volume']) * recency_weight
            chip_data.append({'price': bin_mid, 'volume': round(weighted_vol, 0)})

        # Cost estimates at key periods
        def _cost(n):
            tail = df_recent.tail(n)
            total_vol = float(tail['volume'].sum())
            if total_vol == 0:
                return 0.0
            return round(float((tail['close'] * tail['volume']).sum() / total_vol), 2)

        return jsonify({
            'success': True,
            'chip_distribution': chip_data,
            'major_cost': {
                'cost_20d': _cost(20),
                'cost_60d': _cost(60),
                'cost_120d': _cost(120),
            }
        })
    except Exception as e:
        logger.error(f"api_chip_distribution error: {e}")
        return jsonify({'success': False, 'error': str(e)})


# ============== 股票分组API ==============

GROUPS_FILE = os.path.join(config.get('data_storage.data_dir', './data'), 'groups.json')


def _load_groups():
    if os.path.exists(GROUPS_FILE):
        try:
            with open(GROUPS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {'groups': {}}


def _save_groups(data):
    os.makedirs(os.path.dirname(GROUPS_FILE), exist_ok=True)
    with open(GROUPS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


@app.route('/api/groups', methods=['GET', 'POST'])
def api_groups():
    """列出所有分组 (GET) 或创建新分组 (POST)"""
    try:
        if request.method == 'GET':
            data = _load_groups()
            return jsonify({'success': True, 'groups': list(data['groups'].values())})

        # POST: create new group
        body = request.json or {}
        name = body.get('name', '').strip()
        stocks = body.get('stocks', [])
        if not name:
            return jsonify({'success': False, 'error': '分组名称不能为空'}), 400
        data = _load_groups()
        data['groups'][name] = {'name': name, 'stocks': stocks}
        _save_groups(data)
        return jsonify({'success': True, 'group': data['groups'][name]})
    except Exception as e:
        logger.error(f"api_groups error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/groups/<name>', methods=['DELETE'])
def api_delete_group(name):
    """删除分组"""
    try:
        data = _load_groups()
        if name not in data['groups']:
            return jsonify({'success': False, 'error': '分组不存在'}), 404
        del data['groups'][name]
        _save_groups(data)
        return jsonify({'success': True, 'message': f'分组 {name} 已删除'})
    except Exception as e:
        logger.error(f"api_delete_group error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/groups/<name>/chart')
def api_group_chart(name):
    """获取分组合成收益图（按权重加权）"""
    try:
        data = _load_groups()
        group = data['groups'].get(name)
        if not group:
            return jsonify({'success': False, 'error': '分组不存在'}), 404

        stocks = group.get('stocks', [])
        if not stocks:
            return jsonify({'success': False, 'error': '分组内无股票'}), 404

        start_date = request.args.get('start', (datetime.now() - timedelta(days=365)).strftime('%Y%m%d'))
        end_date = request.args.get('end', datetime.now().strftime('%Y%m%d'))

        combined = None
        total_weight = sum(float(s.get('weight', 1)) for s in stocks)

        for stock_info in stocks:
            code = stock_info.get('code')
            weight = float(stock_info.get('weight', 1)) / total_weight if total_weight > 0 else 1.0 / len(stocks)
            df = unified_data.get_historical_data(code, start_date, end_date, freq='day')
            if df is None or df.empty:
                continue
            df = df.copy()
            df['date'] = pd.to_datetime(df['date'].astype(str), errors='coerce')
            df = df.dropna(subset=['date']).sort_values('date')
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df = df.dropna(subset=['close'])
            base = df['close'].iloc[0]
            df['return_pct'] = (df['close'] / base - 1) * 100 * weight
            df_ret = df[['date', 'return_pct']].set_index('date')

            if combined is None:
                combined = df_ret.rename(columns={'return_pct': 'combined'})
            else:
                combined = combined.join(df_ret, how='outer', rsuffix='_new')
                combined['combined'] = combined['combined'].add(combined.get('return_pct_new', 0), fill_value=0)
                if 'return_pct_new' in combined.columns:
                    combined = combined.drop(columns=['return_pct_new'])

        if combined is None or combined.empty:
            return jsonify({'success': False, 'error': '无法获取分组数据'}), 404

        combined = combined.reset_index()
        combined['date'] = combined['date'].dt.strftime('%Y-%m-%d')

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=combined['date'].tolist(),
            y=combined['combined'].tolist(),
            name=f'{name} 组合收益(%)',
            line=dict(color='#2196f3', width=2),
            fill='tozeroy',
            fillcolor='rgba(33,150,243,0.1)',
            hovertemplate='日期: %{x}<br>收益: %{y:.2f}%<extra></extra>'
        ))
        fig.update_layout(
            title=f'{name} - 组合累计收益',
            xaxis_title='日期',
            yaxis_title='累计收益(%)',
            height=450
        )
        chart_json = json.dumps(fig, cls=PlotlyJSONEncoder)
        return jsonify({'success': True, 'chart': chart_json})
    except Exception as e:
        logger.error(f"api_group_chart error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============== 系统状态持久化 ==============

def save_system_state():
    """保存系统状态到文件"""
    try:
        state = {
            'risk_manager': risk_manager.to_dict(),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(DATA_STATE_PATH, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        
        logger.info("系统状态已保存")
    except Exception as e:
        logger.error(f"保存系统状态失败: {e}")


def load_system_state():
    """从文件加载系统状态"""
    try:
        if os.path.exists(DATA_STATE_PATH):
            with open(DATA_STATE_PATH, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            if 'risk_manager' in state:
                risk_manager.from_dict(state['risk_manager'])
            
            logger.info("系统状态已加载")
    except Exception as e:
        logger.error(f"加载系统状态失败: {e}")


STRATEGIES_FILE = os.path.join(config.get('data_storage.data_dir', './data'), 'strategies.json')

def save_strategies_to_file():
    """保存策略到文件"""
    try:
        strategies_data = {}
        for name, strategy in strategy_manager.strategies.items():
            strategies_data[name] = strategy.to_dict()
        
        os.makedirs(os.path.dirname(STRATEGIES_FILE), exist_ok=True)
        tmp_file = STRATEGIES_FILE + '.tmp'
        with open(tmp_file, 'w', encoding='utf-8') as f:
            json.dump(strategies_data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_file, STRATEGIES_FILE)
        
        logger.info("策略已保存到文件")
    except Exception as e:
        logger.error(f"保存策略失败: {e}")
        if os.path.exists(STRATEGIES_FILE + '.tmp'):
            try:
                os.remove(STRATEGIES_FILE + '.tmp')
            except OSError:
                pass


def load_strategies_from_file():
    """从文件加载策略"""
    try:
        if os.path.exists(STRATEGIES_FILE):
            with open(STRATEGIES_FILE, 'r', encoding='utf-8') as f:
                strategies_data = json.load(f)
            
            from .strategy import QuantStrategy
            for name, data in strategies_data.items():
                try:
                    strategy = QuantStrategy.from_dict(data)
                    strategy_manager.add_strategy(name, strategy)
                except Exception as e:
                    logger.error(f"加载策略 {name} 失败: {e}")
            
            logger.info("策略已从文件加载")
    except Exception as e:
        logger.error(f"加载策略失败: {e}")


# ============== 通知 API ==============

@app.route('/api/notification/status')
def api_notification_status():
    """获取通知渠道配置状态"""
    return jsonify(notification_manager.get_config_status())


@app.route('/api/notification/test', methods=['POST'])
def api_notification_test():
    """发送测试通知"""
    try:
        data = request.json or {}
        channel = data.get('channel', 'all')
        channels = [channel] if channel != 'all' else ['wechat', 'email']
        results = notification_manager._send(
            '测试通知 - 量化交易系统',
            f'这是一条测试消息，发送时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            channels=channels
        )
        return jsonify({'success': True, 'results': {k: v for k, v in results.items()}})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/notification/send', methods=['POST'])
def api_notification_send():
    """手动发送自定义通知"""
    try:
        data = request.json or {}
        title = data.get('title', '量化系统通知')
        content = data.get('content', '')
        channels = data.get('channels', ['wechat', 'email'])
        if not content:
            return jsonify({'error': '消息内容不能为空'}), 400
        results = notification_manager._send(title, content, channels=channels)
        return jsonify({'success': True, 'results': {k: v for k, v in results.items()}})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/notification/config', methods=['POST'])
def api_notification_config():
    """保存通知配置"""
    try:
        data = request.json or {}
        
        # Update PushPlus token
        token = data.get('pushplus_token', '').strip()
        if token:
            config.set('tokens.pushplus_token', token)
            notification_manager.notifier = PushPlusNotifier()
            notification_manager.wechat_enabled = True
        
        # Update email config
        smtp_host = data.get('smtp_host', '').strip()
        if smtp_host:
            config.set('notification.email.smtp_host', smtp_host)
            config.set('notification.email.smtp_port', data.get('smtp_port', 465))
            config.set('notification.email.sender', data.get('sender', ''))
            config.set('notification.email.password', data.get('password', ''))
            config.set('notification.email.receiver', data.get('receiver', ''))
            from .notification import EmailNotifier
            notification_manager.email_notifier = EmailNotifier()
            notification_manager.email_enabled = notification_manager.email_notifier.is_configured
        
        notification_manager.enabled = notification_manager.wechat_enabled or notification_manager.email_enabled
        config.save()
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============== 策略提醒页面 & API ==============

@app.route('/strategy/alerts')
def strategy_alerts_page():
    """策略提醒子页面"""
    return render_template('strategy_alerts.html')


def _backtest_one_strategy(code, strategy_name):
    """使用真实回测引擎计算单只股票指定策略的收益率(%)"""
    strategy_obj = strategy_manager.get_strategy(strategy_name)
    if strategy_obj is None:
        return 0.0
    from datetime import timedelta
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    try:
        result = backtest_engine.run_backtest(code, strategy_obj, start_date, end_date, initial_capital=1000000)
        return result.total_return_pct if result else 0.0
    except Exception as e:
        logger.warning(f"回测失败 {code} {strategy_name}: {e}")
        return 0.0


def _backtest_one_strategy_with_end(code, strategy_name, end_date_str, sell_strategy_name=None):
    """使用真实回测引擎，指定截止日期；支持独立的买入/卖出策略"""
    buy_obj = strategy_manager.get_strategy(strategy_name)
    if buy_obj is None:
        return 0.0
    if sell_strategy_name and sell_strategy_name != strategy_name:
        sell_obj = strategy_manager.get_strategy(sell_strategy_name)
        from .strategy import merge_buy_sell_strategies
        strategy_obj = merge_buy_sell_strategies(buy_obj, sell_obj) if sell_obj else buy_obj
    else:
        strategy_obj = buy_obj
    from datetime import timedelta
    end_dt = datetime.strptime(end_date_str, '%Y%m%d')
    start_date = (end_dt - timedelta(days=365)).strftime('%Y%m%d')
    try:
        result = backtest_engine.run_backtest(
            code, strategy_obj, start_date, end_date_str,
            initial_capital=1000000,
        )
        return result.total_return_pct if result else 0.0
    except Exception as e:
        logger.warning(f"回测失败 {code} {strategy_name}: {e}")
        return 0.0


@app.route('/api/strategy/alerts/assign', methods=['POST'])
def api_strategy_alerts_assign():
    """保存股票策略分配到YAML（支持独立的买入/卖出策略）"""
    try:
        data = request.get_json(force=True)
        code = data.get('code', '')
        buy_strategy_name = data.get('buy_strategy') or data.get('strategy', '')
        sell_strategy_name = data.get('sell_strategy', '')
        stock = stock_manager.get_stock_by_code(code)
        if stock is None:
            return jsonify({'success': False, 'error': '股票不存在'}), 404
        stock.buy_strategy = buy_strategy_name
        stock.sell_strategy = sell_strategy_name
        # 向后兼容：strategy字段存买入策略
        stock.strategy = buy_strategy_name
        stock_manager.save()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"策略分配保存失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/strategy/alerts/compute', methods=['POST'])
def api_strategy_alerts_compute():
    """计算单只股票指定策略的回测收益(今天/昨天)和当前建议"""
    try:
        data = request.get_json(force=True)
        code = data.get('code', '')
        buy_strategy_name = data.get('buy_strategy') or data.get('strategy', 'RSI策略')
        sell_strategy_name = data.get('sell_strategy') or buy_strategy_name
        from datetime import timedelta

        stock = stock_manager.get_stock_by_code(code)
        if stock is None:
            return jsonify({'success': False, 'error': f'股票不存在: {code}'}), 404

        end_today = datetime.now().strftime('%Y%m%d')
        end_yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        return_today = _backtest_one_strategy_with_end(stock.full_code, buy_strategy_name, end_today, sell_strategy_name)
        return_yesterday = _backtest_one_strategy_with_end(stock.full_code, buy_strategy_name, end_yesterday, sell_strategy_name)

        # 获取当前信号
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        df = unified_data.get_historical_data(stock.full_code, start_date=start_date)
        signal = '无信号'
        if df is not None and not df.empty and len(df) >= 2:
            df_ind = technical_indicators.calculate_all_indicators_from_df(df.copy())
            if not df_ind.empty and len(df_ind) >= 2:
                latest = df_ind.iloc[-1]
                prev = df_ind.iloc[-2]
                signal = scheduler._get_current_signal(buy_strategy_name, latest, prev)

        return jsonify({
            'success': True,
            'code': code,
            'name': stock.name,
            'strategy': buy_strategy_name,
            'sell_strategy': sell_strategy_name if sell_strategy_name != buy_strategy_name else '',
            'return_today': round(return_today, 2),
            'return_yesterday': round(return_yesterday, 2),
            'signal': signal,
        })
    except Exception as e:
        logger.error(f"策略提醒计算失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/strategy/alerts/compute-all', methods=['POST'])
def api_strategy_alerts_compute_all():
    """批量计算所有股票的策略回测"""
    try:
        data = request.get_json(force=True)
        items = data.get('items', [])
        from datetime import timedelta
        end_today = datetime.now().strftime('%Y%m%d')
        end_yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        results = []

        for item in items:
            code = item.get('code', '')
            buy_strategy_name = item.get('buy_strategy') or item.get('strategy', 'RSI策略')
            sell_strategy_name = item.get('sell_strategy') or buy_strategy_name
            try:
                stock = stock_manager.get_stock_by_code(code)
                if stock is None:
                    results.append({'code': code, 'success': False, 'error': '不存在'})
                    continue

                return_today = _backtest_one_strategy_with_end(stock.full_code, buy_strategy_name, end_today, sell_strategy_name)
                return_yesterday = _backtest_one_strategy_with_end(stock.full_code, buy_strategy_name, end_yesterday, sell_strategy_name)

                # 获取当前信号
                signal = '无信号'
                df = unified_data.get_historical_data(stock.full_code, start_date=start_date)
                if df is not None and not df.empty and len(df) >= 2:
                    df_ind = technical_indicators.calculate_all_indicators_from_df(df.copy())
                    if not df_ind.empty and len(df_ind) >= 2:
                        latest = df_ind.iloc[-1]
                        prev = df_ind.iloc[-2]
                        signal = scheduler._get_current_signal(buy_strategy_name, latest, prev)

                results.append({
                    'code': code,
                    'name': stock.name,
                    'success': True,
                    'strategy': buy_strategy_name,
                    'sell_strategy': sell_strategy_name if sell_strategy_name != buy_strategy_name else '',
                    'return_today': round(return_today, 2),
                    'return_yesterday': round(return_yesterday, 2),
                    'signal': signal,
                })
            except Exception as e:
                results.append({'code': code, 'success': False, 'error': str(e)})

        return jsonify({'success': True, 'results': results})
    except Exception as e:
        logger.error(f"批量策略计算失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/strategy/alerts/send', methods=['POST'])
def api_strategy_alerts_send():
    """将策略提醒结果发送到微信"""
    try:
        data = request.get_json(force=True)
        items = data.get('items', [])
        if not items:
            return jsonify({'success': False, 'error': '无数据可发送'})

        date_str = datetime.now().strftime('%Y-%m-%d')
        content = f"## 📈 策略提醒 ({date_str})\n\n"
        for item in items:
            ret_today = item.get('return_today', 0)
            ret_yesterday = item.get('return_yesterday', 0)
            diff = ret_today - ret_yesterday
            ret_sign = '+' if ret_today >= 0 else ''
            diff_sign = '+' if diff >= 0 else ''
            content += (
                f"**{item.get('name', '')}({item.get('code', '')})**  "
                f"{item.get('strategy', '')}  "
                f"{ret_sign}{ret_today:.2f}% (较昨日{diff_sign}{diff:.2f}%)  "
                f"**{item.get('signal', '')}**\n\n"
            )

        notification_manager.send_markdown_message(f"策略提醒 {date_str}", content)
        return jsonify({'success': True, 'message': '已发送'})
    except Exception as e:
        logger.error(f"发送策略提醒失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============== AI 日报 API ==============

@app.route('/api/ai/daily-report')
def api_ai_daily_report():
    """生成AI每日总结和建议"""
    try:
        stocks = stock_manager.get_stocks()
        report_items = []
        
        for stock in stocks:
            try:
                code = stock.code
                df = unified_data.get_historical_data(stock.full_code)
                if df is None or df.empty or len(df) < 5:
                    continue
                
                df_ind = technical_indicators.calculate_all_indicators_from_df(df)
                if df_ind.empty:
                    continue
                
                latest = df_ind.iloc[-1]
                prev = df_ind.iloc[-2] if len(df_ind) >= 2 else latest
                
                close = float(latest.get('close', 0) or 0)
                prev_close = float(prev.get('close', 0) or close)
                chg_pct = (close - prev_close) / prev_close * 100 if prev_close > 0 else 0
                
                rsi6 = float(latest.get('rsi_6', 50) or 50)
                macd_val = float(latest.get('macd', 0) or 0)
                macd_sig = float(latest.get('macd_signal', 0) or 0)
                ma5 = float(latest.get('ma_5', close) or close)
                ma20 = float(latest.get('ma_20', close) or close)
                
                # 生成个股建议
                signals = []
                if rsi6 < 30:
                    signals.append('RSI超卖，关注反弹机会')
                elif rsi6 > 70:
                    signals.append('RSI超买，注意回调风险')
                if macd_val > macd_sig and float(prev.get('macd', 0) or 0) <= float(prev.get('macd_signal', 0) or 0):
                    signals.append('MACD金叉，看涨信号')
                elif macd_val < macd_sig and float(prev.get('macd', 0) or 0) >= float(prev.get('macd_signal', 0) or 0):
                    signals.append('MACD死叉，看跌信号')
                if close > ma5 > ma20:
                    signals.append('多头排列，趋势向好')
                elif close < ma5 < ma20:
                    signals.append('空头排列，趋势偏弱')
                
                if rsi6 < 35 and close > ma20:
                    suggestion = '建议关注买入'
                elif rsi6 > 65 and close < ma20:
                    suggestion = '建议减仓观望'
                elif macd_val > macd_sig and close > ma20:
                    suggestion = '持有/加仓'
                else:
                    suggestion = '观望等待'
                
                report_items.append({
                    'code': code,
                    'name': stock.name,
                    'industry': stock.industry,
                    'close': round(close, 2),
                    'change_pct': round(chg_pct, 2),
                    'rsi_6': round(rsi6, 2),
                    'macd': round(macd_val, 4),
                    'signals': signals,
                    'suggestion': suggestion,
                })
            except Exception as e:
                logger.debug(f"日报生成 {stock.code} 失败: {e}")
                continue
        
        # 市场概览
        bullish_count = sum(1 for r in report_items if '多头' in str(r['signals']) or r['change_pct'] > 1)
        bearish_count = sum(1 for r in report_items if '空头' in str(r['signals']) or r['change_pct'] < -1)
        
        if bullish_count > bearish_count * 1.5:
            market_sentiment = '偏多'
            market_advice = '市场整体偏强，可适当增加仓位'
        elif bearish_count > bullish_count * 1.5:
            market_sentiment = '偏空'
            market_advice = '市场整体偏弱，建议控制仓位'
        else:
            market_sentiment = '震荡'
            market_advice = '市场方向不明，建议谨慎操作'
        
        report = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'market_overview': {
                'sentiment': market_sentiment,
                'advice': market_advice,
                'bullish_count': bullish_count,
                'bearish_count': bearish_count,
                'total_stocks': len(report_items),
            },
            'stocks': report_items,
        }
        
        return jsonify({'success': True, 'report': report})
    except Exception as e:
        logger.error(f"生成AI日报失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/ai/daily-report/send', methods=['POST'])
def api_ai_daily_report_send():
    """生成并发送AI日报"""
    try:
        # 先生成日报
        import requests as req_lib
        resp = api_ai_daily_report()
        data = resp.get_json()
        if not data.get('success'):
            return jsonify({'error': '生成日报失败'}), 500
        
        report = data['report']
        
        # 格式化为消息内容
        content = f"## 📊 量化交易日报 ({report['date']})\n\n"
        content += f"### 市场概览\n"
        content += f"- 市场情绪: **{report['market_overview']['sentiment']}**\n"
        content += f"- 操作建议: {report['market_overview']['advice']}\n"
        content += f"- 看涨/看跌: {report['market_overview']['bullish_count']}/{report['market_overview']['bearish_count']}\n\n"
        content += f"### 个股分析\n\n"
        
        for item in report['stocks']:
            chg_sign = '+' if item['change_pct'] >= 0 else ''
            content += f"**{item['name']}({item['code']})** ¥{item['close']} ({chg_sign}{item['change_pct']}%)\n"
            if item['signals']:
                content += f"  信号: {', '.join(item['signals'])}\n"
            content += f"  建议: {item['suggestion']}\n\n"
        
        notification_manager._send(f"量化日报 {report['date']}", content)
        return jsonify({'success': True, 'message': '日报已发送'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============== 大盘分析 & 策略匹配 API ==============

@app.route('/api/market/regime')
def api_market_regime():
    """获取当前大盘环境分析"""
    try:
        from .market_regime import market_regime_detector
        date = request.args.get('date')
        analysis = market_regime_detector.detect(date)
        return jsonify({'success': True, 'data': analysis.to_dict()})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/market/stock-classification')
def api_stock_classification():
    """获取所有股票的自动分类"""
    try:
        from .stock_classifier import stock_classifier
        results = stock_classifier.classify_all()
        return jsonify({
            'success': True,
            'data': {code: c.to_dict() for code, c in results.items()}
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/market/strategy-match')
def api_strategy_match():
    """获取所有股票的策略匹配推荐"""
    try:
        from .strategy_matcher import strategy_matcher
        date = request.args.get('date')
        result = strategy_matcher.analyze_all_stocks(date)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/market/strategy-match/<code>')
def api_strategy_match_stock(code):
    """获取单只股票的策略匹配分析"""
    try:
        from .strategy_matcher import strategy_matcher
        date = request.args.get('date')
        result = strategy_matcher.analyze_stock(code, date)
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/market/strategy-match/send', methods=['POST'])
def api_market_strategy_match_send():
    """将大盘阶段分析结果发送到微信通知"""
    try:
        data = request.json or {}
        analysis = data.get('data', {})
        market = analysis.get('market', {})
        stocks = analysis.get('stocks', [])

        regime_label = market.get('regime_label', '未知')
        score = market.get('score', 0)
        description = market.get('description', '')

        date_str = datetime.now().strftime('%Y-%m-%d %H:%M')
        content = f"## 🏦 大盘阶段判断及策略推荐 ({date_str})\n\n"
        content += f"### 当前大盘阶段：{regime_label}（评分：{score:+.1f}）\n\n"
        if description:
            content += f"> {description}\n\n"

        active = [s for s in stocks if not s.get('is_empty_position')]
        empty = [s for s in stocks if s.get('is_empty_position')]

        if active:
            content += f"### ✅ 适配策略股票（{len(active)} 只）\n\n"
            for s in active[:15]:
                bp = s.get('best_pair', {}) or {}
                cat = (s.get('classification') or {}).get('category_label', '')
                content += f"**{s['name']}({s['code']})** [{cat}]\n"
                content += f"  买入: {bp.get('buy', '-')} | 卖出: {bp.get('sell', '-')}\n\n"

        if empty:
            names = '、'.join(s['name'] for s in empty[:10])
            content += f"### ⚠️ 建议空仓（{len(empty)} 只）\n{names}\n\n"

        notification_manager.send_markdown_message(f"大盘阶段判断 {regime_label}", content)
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"发送大盘分析失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# ============== 页面路由 ==============

@app.route('/dashboard')
def dashboard():
    """仪表盘页面"""
    start_date_raw = config.get('data_collection.history.start_date', '20030101')
    # 格式化 20030101 → 2003-01-01
    try:
        sd = str(start_date_raw).replace('-', '').replace('/', '')[:8]
        start_date_display = f"{sd[:4]}-{sd[4:6]}-{sd[6:8]}"
    except Exception:
        start_date_display = str(start_date_raw)
    return render_template('dashboard.html', data_start_date=start_date_display)


@app.route('/stock/<code>')
def stock_detail(code):
    """股票详情页面"""
    stock = stock_manager.get_stock_by_code(code)
    return render_template('stock_detail.html', code=code, stock=stock)


@app.route('/backtest')
def backtest_page():
    """回测页面"""
    return render_template('backtest.html')


@app.route('/risk')
def risk_page():
    """风控页面"""
    return render_template('risk.html')


@app.route('/strategy')
def strategy_page():
    """策略页面"""
    return render_template('strategy.html')


@app.route('/factors')
def page_factors():
    """因子挖掘页面"""
    return render_template('factors.html')


@app.route('/groups')
def page_groups():
    """自选股组页面"""
    return render_template('groups.html')


@app.route('/report')
def page_report():
    """AI日报页面"""
    return render_template('report.html')


@app.route('/report/market-analysis')
def page_report_market_analysis():
    """大盘阶段判断及对应策略推荐页面"""
    return render_template('report_market_analysis.html')


@app.route('/scheduler')
def page_scheduler():
    """定时任务调度器页面"""
    return render_template('scheduler.html')


@app.route('/api/stock/<code>/factors')
def api_stock_factors(code):
    """综合评分 v2.0 - 价值体系(V1-V7) + 趋势体系(T1-T7)"""
    try:
        df = unified_data.get_historical_data(code)
        if df is None or df.empty:
            return jsonify({'error': 'No data'}), 404

        if 'trade_date' in df.columns and 'date' not in df.columns:
            df = df.rename(columns={'trade_date': 'date'})
        if 'vol' in df.columns and 'volume' not in df.columns:
            df = df.rename(columns={'vol': 'volume'})
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'].astype(str), errors='coerce')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        from .indicators import technical_indicators
        df_ind = technical_indicators.calculate_all_indicators_from_df(df)

        if df_ind.empty or len(df_ind) < 20:
            return jsonify({'error': 'Insufficient indicator data'}), 404

        s = _scoring_core(df_ind)

        # 根据主导类型选择展示维度
        is_value   = s['dominant'] in ('价值主导', '趋势+价值均衡')
        is_trend   = s['dominant'] in ('趋势主导', '趋势+价值均衡')

        # Value sub-dimensions
        val_dims = {
            'V1': {
                'label': 'V1·估值极底', 'score': s['V1'], 'weight': '15%',
                'detail': (
                    f"PE={s['pe_ttm']:.1f}(历史{s['pe_hist_pct']}%分位)"
                    + (f", PB={s['pb']:.2f}(历史{s['pb_hist_pct']}%分位)" if s['pb'] is not None and s['pb_hist_pct'] is not None else '')
                    if s['pe_ttm'] is not None and s['pe_hist_pct'] is not None
                    else ('PE亏损' if s['pe_ttm'] is not None and s['pe_ttm'] < 0 else '无估值数据')
                )
            },
            'V2': {
                'label': 'V2·多周期超卖', 'score': s['V2'], 'weight': '22%',
                'detail': f"RSI6={s['rsi6']:.1f} RSI12={s['rsi12']:.1f} RSI24={s['rsi24']:.1f}"
            },
            'V3': {
                'label': 'V3·历史价格底部', 'score': s['V3'], 'weight': '15%',
                'detail': f"500日区间{s['price_pos_pct']:.0f}%位置"
            },
            'V4': {
                'label': 'V4·深度调整超跌', 'score': s['V4'], 'weight': '12%',
                'detail': f"60日{s['m60']:+.1f}% / 20日{s['m20']:+.1f}%"
            },
            'V5': {
                'label': 'V5·筑底信号', 'score': s['V5'], 'weight': '15%',
                'detail': ', '.join(s['v5_details'])
            },
            'V6': {
                'label': 'V6·空头钝化', 'score': s['V6'], 'weight': '13%',
                'detail': ', '.join(s['v6_details'])
            },
            'V7': {
                'label': 'V7·形态安全边际', 'score': s['V7'], 'weight': '8%',
                'detail': ', '.join(s['v7_details'])
            },
        }

        # Trend sub-dimensions
        trnd_dims = {
            'T1': {
                'label': 'T1·估值合理', 'score': s['T1'], 'weight': '12%',
                'detail': (
                    f"PE={s['pe_ttm']:.1f}(历史{s['pe_hist_pct']}%分位)"
                    if s['pe_ttm'] is not None and s['pe_hist_pct'] is not None
                    else '无估值数据'
                )
            },
            'T2': {
                'label': 'T2·趋势强度', 'score': s['T2'], 'weight': '22%',
                'detail': ', '.join(s['t2_details'])
            },
            'T3': {
                'label': 'T3·动量强度', 'score': s['T3'], 'weight': '18%',
                'detail': f"5日{s['m5']:+.1f}% / 20日{s['m20']:+.1f}% / 60日{s['m60']:+.1f}%"
            },
            'T4': {
                'label': 'T4·RSI健康区间', 'score': s['T4'], 'weight': '12%',
                'detail': f"RSI6={s['rsi6']:.1f} RSI12={s['rsi12']:.1f}"
            },
            'T5': {
                'label': 'T5·对空钝化', 'score': s['T5'], 'weight': '13%',
                'detail': ', '.join(s['t5_details'])
            },
            'T6': {
                'label': 'T6·趋势形态', 'score': s['T6'], 'weight': '13%',
                'detail': ', '.join(s['t6_details'])
            },
            'T7': {
                'label': 'T7·量价配合', 'score': s['T7'], 'weight': '10%',
                'detail': ', '.join(s['t7_details']) + f" (量比={s['vol_ratio']:.2f})"
            },
        }

        # Show dominant system's dims first, then the other system
        if s['dominant'] == '趋势主导':
            dimensions = {**trnd_dims, **val_dims}
        else:
            dimensions = {**val_dims, **trnd_dims}

        return jsonify({
            'success': True,
            'code': code,
            'total_score': s['total_score'],
            'rating': s['rating'],
            'rating_color': s['rating_color'],
            'dominant': s['dominant'],
            'popular_score': s['popular_score'],   # trend score (JS backward compat)
            'value_score': s['value_score'],
            'dimensions': dimensions,
            'core_scores': {
                'value_score': s['value_score'],
                'trend_score': s['trend_score_total'],
                'value_weights': {'V1':0.15,'V2':0.22,'V3':0.15,'V4':0.12,'V5':0.15,'V6':0.13,'V7':0.08},
                'trend_weights': {'T1':0.12,'T2':0.22,'T3':0.18,'T4':0.12,'T5':0.13,'T6':0.13,'T7':0.10},
            },
            'factors': {
                'rsi_6': s['rsi6'], 'rsi_12': s['rsi12'], 'rsi_24': s['rsi24'],
                'macd': s['macd_val'], 'macd_signal': s['macd_sig'],
                'boll_pos': s['boll_pos'],
                'momentum_5d': s['m5'], 'momentum_20d': s['m20'], 'momentum_60d': s['m60'],
                'volume_ratio': s['vol_ratio'],
                'kdj_j': s['kdj_j'], 'wr_14': s['wr14'],
                'price_pos_500d': s['price_pos_pct'],
                'pe_ttm': s['pe_ttm'], 'pb': s['pb'],
                'pe_hist_pct': s['pe_hist_pct'], 'pb_hist_pct': s['pb_hist_pct'],
            }
        })
    except Exception as e:
        logger.error(f"api_stock_factors error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/factors/screen', methods=['POST'])
def api_factors_screen():
    """Screen stocks by factor conditions"""
    try:
        req = request.json or {}
        min_momentum_20d = req.get('min_momentum_20d', -999)
        max_rsi_12 = req.get('max_rsi_12', 100)
        min_rsi_12 = req.get('min_rsi_12', 0)
        min_ma_trend = req.get('min_ma_trend', -999)
        
        results = []
        stocks = stock_manager.get_all_stocks()
        for stock in stocks:
            if stock.type != 'stock':
                continue
            try:
                df = unified_data.get_historical_data(stock.code)
                if df is None or df.empty or len(df) < 25:
                    continue
                
                from .indicators import technical_indicators
                df_ind = technical_indicators.calculate_all_indicators_from_df(df.tail(100))
                if df_ind.empty:
                    continue
                
                latest = df_ind.iloc[-1]
                close = float(latest.get('close', 0) or 0)
                close_20d = float(df_ind.iloc[-21].get('close', close) or close) if len(df_ind) >= 21 else close
                
                momentum_20d = (close / close_20d - 1) * 100 if close_20d > 0 else 0
                rsi_12 = float(latest.get('rsi_12', 50) or 50)
                ma5 = float(latest.get('ma_5', close) or close)
                ma20 = float(latest.get('ma_20', close) or close)
                ma_trend = (ma5 / ma20 - 1) * 100 if ma20 > 0 else 0
                
                if (momentum_20d >= min_momentum_20d and 
                    min_rsi_12 <= rsi_12 <= max_rsi_12 and
                    ma_trend >= min_ma_trend):
                    results.append({
                        'code': stock.code,
                        'name': stock.name,
                        'close': round(close, 2),
                        'momentum_20d': round(momentum_20d, 2),
                        'rsi_12': round(rsi_12, 2),
                        'ma_trend': round(ma_trend, 2),
                        'signal': 'bullish' if momentum_20d > 5 and ma_trend > 1 else 'neutral'
                    })
            except Exception:
                continue
        
        results.sort(key=lambda x: x['momentum_20d'], reverse=True)
        return jsonify({'success': True, 'results': results[:50]})
    except Exception as e:
        logger.error(f"api_factors_screen error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/factors/valuation')
def api_factors_valuation():
    """RSI percentile valuation analysis: short=100日RSI, mid=100周RSI, long=100月RSI"""
    try:
        import numpy as np
        from .indicators import fresh_technical_indicators
        
        results = []
        stocks = stock_manager.get_all_stocks()
        for stock in stocks:
            if stock.type != 'stock':
                continue
            try:
                df = unified_data.get_historical_data(stock.code)
                if df is None or df.empty or len(df) < 30:
                    continue
                
                # Ensure date column is datetime
                df = df.copy()
                if 'date' not in df.columns:
                    continue
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date').reset_index(drop=True)
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                def safe(v, default=0.0):
                    try:
                        f = float(v)
                        return f if not (f != f) else default
                    except Exception:
                        return default
                
                close = safe(df['close'].iloc[-1])
                
                # --- Short-term: 100-day RSI percentile ---
                rsi_daily = fresh_technical_indicators.calculate_simple_rsi(df['close'], period=6)
                current_rsi_daily = safe(rsi_daily.iloc[-1], 50)
                short_window = min(len(rsi_daily), 100)
                rsi_daily_hist = rsi_daily.iloc[-short_window:].dropna()
                if len(rsi_daily_hist) >= 20:
                    short_pctile = float((rsi_daily_hist < current_rsi_daily).sum()) / len(rsi_daily_hist) * 100
                else:
                    short_pctile = 50
                
                # --- Mid-term: 100-week RSI percentile ---
                df_weekly = resample_to_weekly(df.copy())
                mid_pctile = 50
                current_rsi_weekly = 50
                if len(df_weekly) >= 15:
                    rsi_weekly = fresh_technical_indicators.calculate_simple_rsi(df_weekly['close'], period=6)
                    current_rsi_weekly = safe(rsi_weekly.iloc[-1], 50)
                    mid_window = min(len(rsi_weekly), 100)
                    rsi_weekly_hist = rsi_weekly.iloc[-mid_window:].dropna()
                    if len(rsi_weekly_hist) >= 10:
                        mid_pctile = float((rsi_weekly_hist < current_rsi_weekly).sum()) / len(rsi_weekly_hist) * 100
                
                # --- Long-term: 100-month RSI percentile ---
                df_monthly = resample_to_monthly(df.copy())
                long_pctile = 50
                current_rsi_monthly = 50
                if len(df_monthly) >= 10:
                    rsi_monthly = fresh_technical_indicators.calculate_simple_rsi(df_monthly['close'], period=6)
                    current_rsi_monthly = safe(rsi_monthly.iloc[-1], 50)
                    long_window = min(len(rsi_monthly), 100)
                    rsi_monthly_hist = rsi_monthly.iloc[-long_window:].dropna()
                    if len(rsi_monthly_hist) >= 5:
                        long_pctile = float((rsi_monthly_hist < current_rsi_monthly).sum()) / len(rsi_monthly_hist) * 100
                
                # Valuation labels
                def val_label(pctile):
                    if pctile < 10: return '极度低估'
                    elif pctile < 30: return '低估'
                    elif pctile < 70: return '合理'
                    elif pctile < 90: return '高估'
                    else: return '极度高估'
                
                short_val = val_label(short_pctile)
                mid_val = val_label(mid_pctile)
                long_val = val_label(long_pctile)
                
                # Trend: daily RSI direction
                if len(rsi_daily_hist) >= 10:
                    rsi_recent = rsi_daily_hist.iloc[-5:].mean()
                    rsi_prev = rsi_daily_hist.iloc[-10:-5].mean()
                    if rsi_recent > 60 and rsi_recent > rsi_prev:
                        trend, trend_icon = '上升趋势', '📈'
                    elif rsi_recent < 40 and rsi_recent < rsi_prev:
                        trend, trend_icon = '下降趋势', '📉'
                    elif rsi_recent > rsi_prev + 5:
                        trend, trend_icon = '转强', '↗️'
                    elif rsi_recent < rsi_prev - 5:
                        trend, trend_icon = '转弱', '↘️'
                    else:
                        trend, trend_icon = '震荡', '↔️'
                else:
                    trend, trend_icon = '数据不足', '❓'
                
                # Composite score (lower = more undervalued = better buy)
                val_score = round(100 - (short_pctile * 0.3 + mid_pctile * 0.35 + long_pctile * 0.35), 1)
                
                results.append({
                    'code': stock.code,
                    'name': stock.name,
                    'close': round(close, 2),
                    'rsi_daily': round(current_rsi_daily, 2),
                    'short_pctile': round(short_pctile, 1),
                    'short_val': short_val,
                    'rsi_weekly': round(current_rsi_weekly, 2),
                    'mid_pctile': round(mid_pctile, 1),
                    'mid_val': mid_val,
                    'rsi_monthly': round(current_rsi_monthly, 2),
                    'long_pctile': round(long_pctile, 1),
                    'long_val': long_val,
                    'trend': trend,
                    'trend_icon': trend_icon,
                    'val_score': val_score,
                })
            except Exception:
                continue
        
        results.sort(key=lambda x: x['val_score'], reverse=True)
        return jsonify({'success': True, 'results': results})
    except Exception as e:
        logger.error(f"api_factors_valuation error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/factors/optimize', methods=['POST'])
def api_factors_optimize():
    """因子优化：对指定股票遍历所有策略回测，找出收益最高的因子/策略"""
    try:
        data = request.json or {}
        code = data.get('code')
        start_date = data.get('start_date', '20200101')
        end_date = data.get('end_date', '')
        initial_capital = float(data.get('initial_capital', 1000000))

        if not code:
            return jsonify({'error': '请指定股票代码'}), 400

        if not end_date:
            from datetime import datetime
            end_date = datetime.now().strftime('%Y%m%d')

        # Pre-compute indicator DataFrame ONCE to avoid redundant fetching per strategy
        try:
            precomputed_df = technical_indicators.calculate_all_indicators(code, start_date, end_date)
        except Exception as e:
            logger.error(f"因子优化预计算指标失败 {code}: {e}")
            return jsonify({'error': f'获取数据失败: {e}'}), 500

        if precomputed_df is None or precomputed_df.empty:
            return jsonify({'error': f'无法获取 {code} 的历史数据'}), 400

        # ── 与 run_backtest() 保持一致：补充周线/月线指标 ──
        if 'date' in precomputed_df.columns:
            precomputed_df['date'] = pd.to_datetime(precomputed_df['date'])
        if 'boll_position' not in precomputed_df.columns and 'boll_upper' in precomputed_df.columns:
            import numpy as _np
            _rng = precomputed_df['boll_upper'] - precomputed_df['boll_lower']
            precomputed_df['boll_position'] = (
                (precomputed_df['close'] - precomputed_df['boll_lower']) / _rng.replace(0, _np.nan)
            ).clip(0, 1).fillna(0.5)
        precomputed_df = backtest_engine._merge_weekly_monthly(code, precomputed_df)

        # ── 补充基准指数相对强弱（rel_strength_*）──
        _BENCHMARK = '000001.SH'
        try:
            _idx_raw = unified_data.get_historical_data(_BENCHMARK, start_date, end_date)
            if _idx_raw is not None and not _idx_raw.empty:
                _idx = _idx_raw.copy()
                _idx['date'] = pd.to_datetime(_idx['date'])
                _idx = _idx.sort_values('date').reset_index(drop=True)
                _idx['idx_pct_chg'] = _idx['close'].pct_change() * 100
                for _n in [5, 10, 20, 60]:
                    _idx[f'idx_ret_{_n}'] = _idx['close'].pct_change(_n) * 100
                    precomputed_df[f'stock_ret_{_n}'] = precomputed_df['close'].pct_change(_n) * 100
                _idx_cols = ['date', 'idx_pct_chg'] + [f'idx_ret_{_n}' for _n in [5, 10, 20, 60]]
                precomputed_df = pd.merge_asof(
                    precomputed_df.sort_values('date'), _idx[_idx_cols],
                    on='date', direction='backward'
                )
                for _n in [5, 10, 20, 60]:
                    precomputed_df[f'rel_strength_{_n}'] = (
                        precomputed_df[f'stock_ret_{_n}'] - precomputed_df[f'idx_ret_{_n}']
                    )
            else:
                raise ValueError("指数数据为空")
        except Exception as _bex:
            logger.warning(f"因子优化加载基准指数失败，相对强弱指标默认为0: {_bex}")
            for _n in [5, 10, 20, 60]:
                precomputed_df[f'rel_strength_{_n}'] = 0.0
                precomputed_df[f'idx_ret_{_n}'] = 0.0
            precomputed_df['idx_pct_chg'] = 0.0

        # 按日期范围过滤
        try:
            sd = pd.to_datetime(str(start_date), format='%Y%m%d', errors='coerce')
            ed = pd.to_datetime(str(end_date), format='%Y%m%d', errors='coerce') if end_date else None
            if pd.notna(sd):
                precomputed_df = precomputed_df[precomputed_df['date'] >= sd]
            if ed is not None and pd.notna(ed):
                precomputed_df = precomputed_df[precomputed_df['date'] <= ed]
            precomputed_df = precomputed_df.reset_index(drop=True)
        except Exception as _fe:
            logger.warning(f"因子优化日期过滤失败: {_fe}")

        from .strategy import merge_buy_sell_strategies

        def _json_safe(val, ndigits=2):
            import math
            if isinstance(val, float) and (math.isinf(val) or math.isnan(val)):
                return 0
            return round(val, ndigits)

        # ── 1. 用户自定义策略：按名称结尾分为买入/卖出两组，然后按序配对 ──
        user_buy = []
        user_sell = []
        for key in strategy_manager.list_strategies():
            strat = strategy_manager.get_strategy(key)
            if not strat or not strat.rules:
                continue
            if strat.name.endswith('买入'):
                user_buy.append((key, strat))
            elif strat.name.endswith('卖出'):
                user_sell.append((key, strat))

        # 将买/卖策略逐一配对（按出现顺序），假设数量相同
        user_pairs = []
        for i in range(max(len(user_buy), len(user_sell))):
            buy_item  = user_buy[i]  if i < len(user_buy)  else None
            sell_item = user_sell[i] if i < len(user_sell) else None
            user_pairs.append((buy_item, sell_item))

        # ── 2. 参数化因子策略（自带买卖规则，视作完整策略对）──
        #factor_strategies = _generate_factor_strategies()

        # ── 3. 回测：先跑用户策略对，再跑因子策略 ──
        results = []

        for buy_item, sell_item in user_pairs:
            buy_key,  buy_strat  = buy_item  if buy_item  else (None, None)
            sell_key, sell_strat = sell_item if sell_item else (None, None)
            buy_name  = buy_strat.name  if buy_strat  else '—'
            sell_name = sell_strat.name if sell_strat else '—'
            pair_key  = f"{buy_key or 'none'}+{sell_key or 'none'}"

            if buy_strat and sell_strat:
                merged = merge_buy_sell_strategies(buy_strat, sell_strat)
            elif buy_strat:
                merged = buy_strat
            else:
                merged = sell_strat

            try:
                result = backtest_engine.run_backtest(
                    code, merged, start_date, end_date, initial_capital,
                    precomputed_df=precomputed_df
                )
                results.append({
                    'strategy_key': pair_key,
                    'buy_strategy_name': buy_name,
                    'sell_strategy_name': sell_name,
                    'description': merged.description,
                    'total_return_pct': _json_safe(result.total_return_pct, 2),
                    'annual_return': _json_safe(result.annual_return, 2),
                    'max_drawdown_pct': _json_safe(result.max_drawdown_pct, 2),
                    'sharpe_ratio': _json_safe(result.sharpe_ratio, 4),
                    'total_trades': result.total_trades,
                    'win_rate': _json_safe(result.win_rate, 2),
                    'profit_factor': _json_safe(result.profit_factor, 4),
                })
            except Exception as e:
                logger.debug(f"因子优化回测 {pair_key} 失败: {e}")
                results.append({
                    'strategy_key': pair_key,
                    'buy_strategy_name': buy_name,
                    'sell_strategy_name': sell_name,
                    'description': merged.description if merged else '',
                    'total_return_pct': None,
                    'annual_return': None,
                    'error': str(e),
                })

        # for key, strategy in factor_strategies.items():
        #     try:
        #         result = backtest_engine.run_backtest(
        #             code, strategy, start_date, end_date, initial_capital,
        #             precomputed_df=precomputed_df
        #         )
        #         results.append({
        #             'strategy_key': key,
        #             'buy_strategy_name': strategy.name,
        #             'sell_strategy_name': '（含卖出）',
        #             'description': strategy.description,
        #             'total_return_pct': _json_safe(result.total_return_pct, 2),
        #             'annual_return': _json_safe(result.annual_return, 2),
        #             'max_drawdown_pct': _json_safe(result.max_drawdown_pct, 2),
        #             'sharpe_ratio': _json_safe(result.sharpe_ratio, 4),
        #             'total_trades': result.total_trades,
        #             'win_rate': _json_safe(result.win_rate, 2),
        #             'profit_factor': _json_safe(result.profit_factor, 4),
        #         })
        #     except Exception as e:
        #         logger.debug(f"因子优化回测 {key} 失败: {e}")
        #         results.append({
        #             'strategy_key': key,
        #             'buy_strategy_name': strategy.name,
        #             'sell_strategy_name': '（含卖出）',
        #             'description': strategy.description,
        #             'total_return_pct': None,
        #             'annual_return': None,
        #             'error': str(e),
        #         })

        # Sort by return (None last)
        results.sort(key=lambda x: x.get('total_return_pct') if x.get('total_return_pct') is not None else -9999, reverse=True)

        best = results[0] if results and results[0].get('total_return_pct') is not None else None

        return jsonify({
            'success': True,
            'code': code,
            'start_date': start_date,
            'end_date': end_date,
            'best_strategy': best,
            'results': results,
        })
    except Exception as e:
        logger.error(f"api_factors_optimize error: {e}")
        return jsonify({'error': str(e)}), 500


def _generate_factor_strategies():
    """生成参数化的因子策略用于优化回测"""
    from .strategy import QuantStrategy
    
    strategies = {}
    
    # RSI超卖阈值扫描
    for threshold in [20, 25, 30, 35]:
        sell_th = 100 - threshold
        s = QuantStrategy(
            name=f"RSI({threshold}/{sell_th})",
            description=f"RSI低于{threshold}买入，高于{sell_th}卖出"
        )
        s.add_rule(f"rsi_6 < {threshold}", "buy", 0.5, f"RSI6<{threshold}")
        s.add_rule(f"rsi_6 > {sell_th}", "sell", 1.0, f"RSI6>{sell_th}")
        strategies[f'opt_rsi_{threshold}'] = s
    
    # RSI12变体
    for threshold in [25, 30, 35, 40]:
        sell_th = 100 - threshold
        s = QuantStrategy(
            name=f"RSI12({threshold}/{sell_th})",
            description=f"RSI12低于{threshold}买入，高于{sell_th}卖出"
        )
        s.add_rule(f"rsi_12 < {threshold}", "buy", 0.5, f"RSI12<{threshold}")
        s.add_rule(f"rsi_12 > {sell_th}", "sell", 1.0, f"RSI12>{sell_th}")
        strategies[f'opt_rsi12_{threshold}'] = s
    
    # 均线策略变体
    for fast, slow in [(5, 20), (5, 60), (10, 60), (20, 60)]:
        s = QuantStrategy(
            name=f"MA{fast}/{slow}金叉",
            description=f"MA{fast}上穿MA{slow}买入，下穿卖出"
        )
        s.add_rule(f"ma_{fast} > ma_{slow}", "buy", 0.6, f"MA{fast}>MA{slow}")
        s.add_rule(f"ma_{fast} < ma_{slow}", "sell", 0.6, f"MA{fast}<MA{slow}")
        strategies[f'opt_ma_{fast}_{slow}'] = s
    
    # MACD变体
    s = QuantStrategy(name="MACD柱状图", description="MACD柱状图>0买入，<0卖出")
    s.add_rule("macd_histogram > 0", "buy", 0.5, "MACD柱>0")
    s.add_rule("macd_histogram < 0", "sell", 0.5, "MACD柱<0")
    strategies['opt_macd_hist'] = s
    
    # 布林带变体
    s = QuantStrategy(name="布林带反弹", description="价格低于下轨买入，高于上轨卖出")
    s.add_rule("close < boll_lower", "buy", 0.5, "触及下轨")
    s.add_rule("close > boll_upper", "sell", 1.0, "触及上轨")
    strategies['opt_boll'] = s
    
    # 组合因子
    s = QuantStrategy(name="RSI+MA组合", description="RSI<35且均线多头买入")
    s.add_rule("rsi_6 < 35 and ma_5 > ma_20", "buy", 0.6, "RSI超卖+趋势向上")
    s.add_rule("rsi_6 > 70", "sell", 1.0, "RSI超买")
    strategies['opt_rsi_ma_combo'] = s
    
    return strategies


# ============== 数据源配置API ==============

@app.route('/api/config/data_source')
def api_config_data_source_get():
    """获取当前数据源配置"""
    return jsonify({
        'success': True,
        'preferred_source': unified_data.preferred_source,
        'tushare_available': unified_data.tushare_available,
        'options': ['easyquotation', 'tushare']
    })


@app.route('/api/config/data_source', methods=['POST'])
def api_config_data_source_set():
    """切换默认数据源"""
    try:
        data = request.json or {}
        source = data.get('source', '').strip()
        if source not in ('easyquotation', 'tushare'):
            return jsonify({'error': '无效的数据源，可选: easyquotation, tushare'}), 400
        
        unified_data.preferred_source = source
        config.set('data_storage.history_source', source)
        
        return jsonify({
            'success': True,
            'message': f'数据源已切换为 {source}',
            'preferred_source': source
        })
    except Exception as e:
        logger.error(f"切换数据源失败: {e}")
        return jsonify({'error': str(e)}), 500


# ============== 上市日期API ==============

@app.route('/api/data/listing_dates/fetch', methods=['POST'])
def api_fetch_listing_dates():
    """从 Tushare 拉取/更新所有 A 股上市日期缓存"""
    try:
        data = request.json or {}
        force = bool(data.get('force', False))
        result = unified_data.fetch_listing_dates(force=force)
        return jsonify({'success': True, 'count': len(result), 'message': f'已缓存 {len(result)} 条上市日期'})
    except Exception as e:
        logger.error(f"拉取上市日期失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/data/listing_dates/<code>')
def api_get_listing_date(code: str):
    """获取单只股票上市日期（缓存未命中时自动从 Tushare 拉取）"""
    try:
        from .code_converter import to_unified_code
        unified = to_unified_code(code) if '.' not in code else code
        list_date = unified_data.get_listing_date(unified)

        # 缓存未命中则尝试从 Tushare 拉取该股票的上市日期
        if not list_date:
            try:
                from config import TUSHARE_TOKEN
                import tushare as ts
                if TUSHARE_TOKEN:
                    ts.set_token(TUSHARE_TOKEN)
                    pro = ts.pro_api(TUSHARE_TOKEN)
                    df = pro.stock_basic(ts_code=unified, fields='ts_code,list_date,name')
                    if df is not None and not df.empty:
                        row = df.iloc[0]
                        ld = str(row.get('list_date', '')) if pd.notna(row.get('list_date')) else ''
                        if ld and len(ld) == 8:
                            unified_data._listing_dates[unified] = ld
                            unified_data._save_listing_dates()
                            list_date = ld
            except Exception as fetch_err:
                logger.debug(f"单股 Tushare 上市日期拉取失败 ({unified}): {fetch_err}")

        # 格式化为 YYYY-MM-DD 方便前端显示
        list_date_fmt = ''
        if list_date and len(list_date) == 8:
            list_date_fmt = f"{list_date[:4]}-{list_date[4:6]}-{list_date[6:]}"

        return jsonify({'code': unified, 'list_date': list_date or '', 'list_date_fmt': list_date_fmt})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============== 策略适配 ==============

@app.route('/strategy_adapt')
@app.route('/strategy/adapt')
def page_strategy_adapt():
    """策略适配页面：对一对买入/卖出策略遍历所有股票回测"""
    return render_template('strategy_adapt.html')


@app.route('/api/strategy/adapt/run', methods=['POST'])
def api_strategy_adapt_run():
    """启动策略适配后台任务，返回 run_id"""
    try:
        data = request.json or {}
        buy_strategy_name = data.get('buy_strategy', '')
        sell_strategy_name = data.get('sell_strategy', '')
        start_date = data.get('start_date', '20200101')
        end_date = data.get('end_date', '') or datetime.now().strftime('%Y%m%d')
        initial_capital = float(data.get('initial_capital', 1000000))

        if not buy_strategy_name and not sell_strategy_name:
            return jsonify({'error': '请至少选择一个策略'}), 400

        buy_strat = strategy_manager.get_strategy(buy_strategy_name) if buy_strategy_name else None
        sell_strat = strategy_manager.get_strategy(sell_strategy_name) if sell_strategy_name else None

        if not buy_strat and not sell_strat:
            return jsonify({'error': '策略不存在，请检查策略名称'}), 400

        from .strategy import merge_buy_sell_strategies
        if buy_strat and sell_strat:
            merged = merge_buy_sell_strategies(buy_strat, sell_strat)
        elif buy_strat:
            merged = buy_strat
        else:
            merged = sell_strat

        import threading, uuid, time as _time
        run_id = str(uuid.uuid4())
        ADAPT_PROGRESS[run_id] = {
            'status': 'running', 'done': 0, 'total': 0,
            'current_stock': '', 'elapsed': 0,
        }

        def _worker():
            import math as _math
            t0 = _time.time()

            def _json_safe(val, nd=2):
                if isinstance(val, float) and (_math.isinf(val) or _math.isnan(val)):
                    return 0
                return round(val, nd) if val is not None else None

            # Pre-load benchmark index data once
            _BENCHMARK = '000001.SH'
            try:
                idx_raw = unified_data.get_historical_data(_BENCHMARK, start_date, end_date)
                if idx_raw is not None and not idx_raw.empty:
                    idx_df = idx_raw.copy()
                    idx_df['date'] = pd.to_datetime(idx_df['date'])
                    idx_df = idx_df.sort_values('date').reset_index(drop=True)
                    for _n in [5, 10, 20, 60]:
                        idx_df[f'idx_ret_{_n}'] = idx_df['close'].pct_change(_n) * 100
                    idx_df['idx_pct_chg'] = idx_df['close'].pct_change() * 100
                    idx_cols = ['date', 'idx_pct_chg'] + [f'idx_ret_{_n}' for _n in [5, 10, 20, 60]]
                    idx_df = idx_df[idx_cols]
                else:
                    idx_df = None
            except Exception:
                idx_df = None

            stocks = [s for s in stock_manager.get_all_stocks()
                      if s.type not in ('index', 'sector')]
            ADAPT_PROGRESS[run_id]['total'] = len(stocks)

            results = []
            for i, stock in enumerate(stocks):
                code = stock.full_code
                ADAPT_PROGRESS[run_id].update({
                    'done': i,
                    'current_stock': f"{stock.name}({stock.code})",
                    'elapsed': round(_time.time() - t0, 1),
                })
                try:
                    df = technical_indicators.calculate_all_indicators(code, start_date, end_date)
                    if df is None or df.empty:
                        continue
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                    if 'boll_position' not in df.columns and 'boll_upper' in df.columns:
                        import numpy as _np
                        _rng = df['boll_upper'] - df['boll_lower']
                        df['boll_position'] = (
                            (df['close'] - df['boll_lower']) / _rng.replace(0, _np.nan)
                        ).clip(0, 1).fillna(0.5)
                    df = backtest_engine._merge_weekly_monthly(code, df)

                    if idx_df is not None:
                        try:
                            for _n in [5, 10, 20, 60]:
                                df[f'stock_ret_{_n}'] = df['close'].pct_change(_n) * 100
                            df = pd.merge_asof(
                                df.sort_values('date'), idx_df,
                                on='date', direction='backward'
                            )
                            for _n in [5, 10, 20, 60]:
                                df[f'rel_strength_{_n}'] = (
                                    df[f'stock_ret_{_n}'] - df[f'idx_ret_{_n}']
                                )
                        except Exception:
                            for _n in [5, 10, 20, 60]:
                                df[f'rel_strength_{_n}'] = 0.0
                    else:
                        for _n in [5, 10, 20, 60]:
                            df[f'rel_strength_{_n}'] = 0.0

                    # Filter date range
                    try:
                        sd = pd.to_datetime(str(start_date), format='%Y%m%d', errors='coerce')
                        ed = pd.to_datetime(str(end_date), format='%Y%m%d', errors='coerce')
                        if pd.notna(sd):
                            df = df[df['date'] >= sd]
                        if pd.notna(ed):
                            df = df[df['date'] <= ed]
                        df = df.reset_index(drop=True)
                    except Exception:
                        pass

                    result = backtest_engine.run_backtest(
                        code, merged, start_date, end_date, initial_capital,
                        precomputed_df=df
                    )
                    results.append({
                        'code': stock.code,
                        'full_code': code,
                        'name': stock.name,
                        'market': stock.market,
                        'total_return_pct': _json_safe(result.total_return_pct, 2),
                        'annual_return': _json_safe(result.annual_return, 2),
                        'max_drawdown_pct': _json_safe(result.max_drawdown_pct, 2),
                        'sharpe_ratio': _json_safe(result.sharpe_ratio, 4),
                        'total_trades': result.total_trades,
                        'win_rate': _json_safe(result.win_rate, 2),
                        'profit_factor': _json_safe(result.profit_factor, 4),
                    })
                except Exception as e:
                    logger.debug(f"策略适配回测 {code} 失败: {e}")
                    results.append({
                        'code': stock.code,
                        'full_code': code,
                        'name': stock.name,
                        'market': getattr(stock, 'market', ''),
                        'total_return_pct': None,
                        'error': str(e),
                    })

            results.sort(
                key=lambda x: x.get('total_return_pct') if x.get('total_return_pct') is not None else -9999,
                reverse=True
            )
            best = next((r for r in results if r.get('total_return_pct') is not None), None)

            ADAPT_RESULTS[run_id] = {
                'status': 'done',
                'buy_strategy': buy_strategy_name,
                'sell_strategy': sell_strategy_name,
                'start_date': start_date,
                'end_date': end_date,
                'results': results,
                'best': best,
                'elapsed': round(_time.time() - t0, 1),
            }
            ADAPT_PROGRESS[run_id].update({
                'status': 'done', 'done': len(stocks),
                'elapsed': round(_time.time() - t0, 1),
            })

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        return jsonify({'run_id': run_id}), 202

    except Exception as e:
        logger.exception(f"api_strategy_adapt_run error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/strategy/adapt/progress')
def api_strategy_adapt_progress():
    """SSE: 策略适配进度流"""
    from flask import Response
    run_id = request.args.get('run_id')
    if not run_id:
        return jsonify({'error': 'Missing run_id'}), 400

    def gen():
        import time as _time, json as _json
        while True:
            info = ADAPT_PROGRESS.get(run_id)
            if info is None:
                yield f"data: {_json.dumps({'status': 'not_found'})}\n\n"
                break
            try:
                yield f"data: {_json.dumps(info)}\n\n"
            except Exception:
                yield "data: {\"status\":\"error\"}\n\n"
            if info.get('status') in ('done', 'error'):
                break
            _time.sleep(1)

    return Response(gen(), mimetype='text/event-stream')


@app.route('/api/strategy/adapt/result')
def api_strategy_adapt_result():
    """获取策略适配结果"""
    run_id = request.args.get('run_id')
    if not run_id:
        return jsonify({'error': 'Missing run_id'}), 400
    res = ADAPT_RESULTS.get(run_id)
    if res is None:
        prog = ADAPT_PROGRESS.get(run_id)
        if prog:
            return jsonify({'status': 'running', 'progress': prog}), 202
        return jsonify({'error': 'run_id not found'}), 404
    return jsonify(res)


# ============== 多策略回测 ==============

# 多策略回测进度与结果存储
MULTI_BT_PROGRESS: Dict[str, Dict] = {}
MULTI_BT_RESULTS: Dict[str, Dict] = {}


@app.route('/strategy/multi_backtest')
def page_multi_backtest():
    """多策略组合回测页面"""
    return render_template('strategy_multi_backtest.html')


@app.route('/api/strategy/multi_backtest/run', methods=['POST'])
def api_multi_backtest_run():
    """
    多策略组合回测：每对(买入策略, 卖出策略, 股票)独立分配资金，共享资金池按比例分配。
    请求体示例：
    {
      "pairs": [
        {"buy_strategy": "中线超跌买入", "sell_strategy": "中线超跌卖出",
         "stock_code": "600519.SH", "allocation": 40},
        ...
      ],
      "start_date": "20240101",
      "end_date": "20250101",
      "total_capital": 1000000
    }
    """
    try:
        data = request.json or {}
        pairs_input = data.get('pairs', [])
        start_date = data.get('start_date', '20200101')
        end_date = data.get('end_date', '') or datetime.now().strftime('%Y%m%d')
        total_capital = float(data.get('total_capital', 1000000))

        if not pairs_input:
            return jsonify({'error': '请至少添加一组策略-股票配对'}), 400

        from .strategy import merge_buy_sell_strategies
        import threading, uuid, time as _time
        run_id = str(uuid.uuid4())
        MULTI_BT_PROGRESS[run_id] = {
            'status': 'running', 'done': 0, 'total': len(pairs_input),
            'current': '', 'elapsed': 0,
        }

        def _worker():
            import math as _math
            t0 = _time.time()

            def _json_safe(val, nd=2):
                if val is None:
                    return None
                if isinstance(val, float) and (_math.isinf(val) or _math.isnan(val)):
                    return 0
                return round(val, nd)

            # Pre-load benchmark once
            _BENCHMARK = '000001.SH'
            try:
                idx_raw = unified_data.get_historical_data(_BENCHMARK, start_date, end_date)
                if idx_raw is not None and not idx_raw.empty:
                    idx_df = idx_raw.copy()
                    idx_df['date'] = pd.to_datetime(idx_df['date'])
                    idx_df = idx_df.sort_values('date').reset_index(drop=True)
                    for _n in [5, 10, 20, 60]:
                        idx_df[f'idx_ret_{_n}'] = idx_df['close'].pct_change(_n) * 100
                    idx_df['idx_pct_chg'] = idx_df['close'].pct_change() * 100
                    idx_cols = ['date', 'idx_pct_chg'] + [f'idx_ret_{_n}' for _n in [5, 10, 20, 60]]
                    idx_df = idx_df[idx_cols]
                else:
                    idx_df = None
            except Exception:
                idx_df = None

            # 标准化 allocation：把各 pair 的百分比归一化为资金分配额
            total_alloc = sum(float(p.get('allocation', 0)) for p in pairs_input)
            if total_alloc <= 0:
                # 均分
                total_alloc = 100.0
                for p in pairs_input:
                    p['allocation'] = 100.0 / len(pairs_input)

            pair_results = []
            # equity curves 按日期对齐用
            all_equity_series = []  # list of {date -> equity_value}

            for i, pair in enumerate(pairs_input):
                buy_name = pair.get('buy_strategy', '')
                sell_name = pair.get('sell_strategy', '')
                stock_code = pair.get('stock_code', '')
                alloc_pct = float(pair.get('allocation', 100.0 / len(pairs_input)))
                pair_capital = total_capital * (alloc_pct / total_alloc)

                MULTI_BT_PROGRESS[run_id].update({
                    'done': i,
                    'current': f"{buy_name}+{sell_name} / {stock_code}",
                    'elapsed': round(_time.time() - t0, 1),
                })

                if not stock_code:
                    pair_results.append({
                        'index': i, 'buy_strategy': buy_name, 'sell_strategy': sell_name,
                        'stock_code': stock_code, 'stock_name': '—', 'allocation_pct': alloc_pct,
                        'pair_capital': pair_capital, 'error': '未指定股票',
                        'total_return_pct': None,
                    })
                    continue

                buy_strat = strategy_manager.get_strategy(buy_name) if buy_name else None
                sell_strat = strategy_manager.get_strategy(sell_name) if sell_name else None

                if not buy_strat and not sell_strat:
                    pair_results.append({
                        'index': i, 'buy_strategy': buy_name, 'sell_strategy': sell_name,
                        'stock_code': stock_code, 'stock_name': '—', 'allocation_pct': alloc_pct,
                        'pair_capital': pair_capital, 'error': '策略不存在',
                        'total_return_pct': None,
                    })
                    continue

                if buy_strat and sell_strat:
                    merged = merge_buy_sell_strategies(buy_strat, sell_strat)
                elif buy_strat:
                    merged = buy_strat
                else:
                    merged = sell_strat

                # 查找股票名称
                stock_obj = stock_manager.get_stock_by_code(stock_code)
                stock_name = stock_obj.name if stock_obj else stock_code

                try:
                    # 准备 precomputed_df
                    full_code = stock_obj.full_code if stock_obj else stock_code
                    df = technical_indicators.calculate_all_indicators(full_code, start_date, end_date)
                    if df is None or df.empty:
                        raise ValueError('无法获取历史数据')

                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                    if 'boll_position' not in df.columns and 'boll_upper' in df.columns:
                        import numpy as _np
                        _rng = df['boll_upper'] - df['boll_lower']
                        df['boll_position'] = (
                            (df['close'] - df['boll_lower']) / _rng.replace(0, _np.nan)
                        ).clip(0, 1).fillna(0.5)
                    df = backtest_engine._merge_weekly_monthly(full_code, df)

                    if idx_df is not None:
                        try:
                            for _n in [5, 10, 20, 60]:
                                df[f'stock_ret_{_n}'] = df['close'].pct_change(_n) * 100
                            df = pd.merge_asof(
                                df.sort_values('date'), idx_df,
                                on='date', direction='backward'
                            )
                            for _n in [5, 10, 20, 60]:
                                df[f'rel_strength_{_n}'] = df[f'stock_ret_{_n}'] - df[f'idx_ret_{_n}']
                        except Exception:
                            for _n in [5, 10, 20, 60]:
                                df[f'rel_strength_{_n}'] = 0.0
                    else:
                        for _n in [5, 10, 20, 60]:
                            df[f'rel_strength_{_n}'] = 0.0

                    # 日期过滤
                    try:
                        sd = pd.to_datetime(str(start_date), format='%Y%m%d', errors='coerce')
                        ed = pd.to_datetime(str(end_date), format='%Y%m%d', errors='coerce')
                        if pd.notna(sd):
                            df = df[df['date'] >= sd]
                        if pd.notna(ed):
                            df = df[df['date'] <= ed]
                        df = df.reset_index(drop=True)
                    except Exception:
                        pass

                    result = backtest_engine.run_backtest(
                        full_code, merged, start_date, end_date, pair_capital,
                        precomputed_df=df
                    )

                    # 收集权益曲线供组合计算（equity_curve 是 DataFrame）
                    ec_df = result.equity_curve
                    if ec_df is not None and not ec_df.empty:
                        ec_records = ec_df.to_dict(orient='records')
                        ec_map = {}
                        for rec in ec_records:
                            d = rec.get('date')
                            if d is not None:
                                d_str = str(d)[:10]
                                ec_map[d_str] = float(rec.get('equity', pair_capital))
                        if ec_map:
                            all_equity_series.append(ec_map)

                    pair_results.append({
                        'index': i,
                        'buy_strategy': buy_name,
                        'sell_strategy': sell_name,
                        'stock_code': stock_code,
                        'full_code': full_code,
                        'stock_name': stock_name,
                        'allocation_pct': _json_safe(alloc_pct, 1),
                        'pair_capital': _json_safe(pair_capital, 0),
                        'total_return_pct': _json_safe(result.total_return_pct, 2),
                        'annual_return': _json_safe(result.annual_return, 2),
                        'max_drawdown_pct': _json_safe(result.max_drawdown_pct, 2),
                        'sharpe_ratio': _json_safe(result.sharpe_ratio, 4),
                        'total_trades': result.total_trades,
                        'win_rate': _json_safe(result.win_rate, 2),
                        'profit_factor': _json_safe(result.profit_factor, 4),
                        'final_equity': _json_safe(result.final_capital, 2),
                    })
                except Exception as e:
                    logger.warning(f"多策略回测 {stock_code} 失败: {e}")
                    import traceback as _tb
                    logger.debug(_tb.format_exc())
                    pair_results.append({
                        'index': i, 'buy_strategy': buy_name, 'sell_strategy': sell_name,
                        'stock_code': stock_code, 'stock_name': stock_name,
                        'allocation_pct': alloc_pct, 'pair_capital': pair_capital,
                        'error': str(e), 'total_return_pct': None,
                    })

            # ── 合并权益曲线 ──
            combined_equity_curve = []
            if all_equity_series:
                all_dates = sorted(set().union(*[ec.keys() for ec in all_equity_series]))
                # 对每个 series，用 forward-fill 填充缺失日期
                filled = []
                for ec_map in all_equity_series:
                    arr = []
                    last_val = None
                    for d in all_dates:
                        if d in ec_map:
                            last_val = ec_map[d]
                        arr.append(last_val)
                    filled.append(arr)

                for di, d in enumerate(all_dates):
                    total_val = sum(
                        (filled[si][di] or 0) for si in range(len(filled))
                    )
                    combined_equity_curve.append({'date': d, 'equity': round(total_val, 2)})

            # ── 组合汇总指标 ──
            valid_pairs = [p for p in pair_results if p.get('total_return_pct') is not None]
            combined_return_pct = None
            combined_annual = None
            combined_max_dd = None
            if combined_equity_curve:
                import numpy as _np
                equities = [c['equity'] for c in combined_equity_curve]
                if equities and equities[0] and equities[0] > 0:
                    combined_return_pct = round((equities[-1] / equities[0] - 1) * 100, 2)
                    days_n = len(equities)
                    years_n = days_n / 252
                    if years_n > 0:
                        combined_annual = round(((equities[-1] / equities[0]) ** (1 / years_n) - 1) * 100, 2)
                    eq_arr = _np.array(equities, dtype=float)
                    cummax = _np.maximum.accumulate(eq_arr)
                    drawdowns = (eq_arr - cummax) / cummax
                    combined_max_dd = round(float(drawdowns.min()) * 100, 2)

            MULTI_BT_RESULTS[run_id] = {
                'status': 'done',
                'start_date': start_date,
                'end_date': end_date,
                'total_capital': total_capital,
                'pair_results': pair_results,
                'combined_equity_curve': combined_equity_curve,
                'combined_return_pct': combined_return_pct,
                'combined_annual': combined_annual,
                'combined_max_dd': combined_max_dd,
                'elapsed': round(_time.time() - t0, 1),
            }
            MULTI_BT_PROGRESS[run_id].update({
                'status': 'done', 'done': len(pairs_input),
                'elapsed': round(_time.time() - t0, 1),
            })

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()
        return jsonify({'run_id': run_id}), 202

    except Exception as e:
        logger.exception(f"api_multi_backtest_run error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/strategy/multi_backtest/progress')
def api_multi_backtest_progress():
    """SSE: 多策略回测进度"""
    from flask import Response
    run_id = request.args.get('run_id')
    if not run_id:
        return jsonify({'error': 'Missing run_id'}), 400

    def gen():
        import time as _time, json as _json
        while True:
            info = MULTI_BT_PROGRESS.get(run_id)
            if info is None:
                yield f"data: {_json.dumps({'status': 'not_found'})}\n\n"
                break
            try:
                yield f"data: {_json.dumps(info)}\n\n"
            except Exception:
                yield "data: {\"status\":\"error\"}\n\n"
            if info.get('status') in ('done', 'error'):
                break
            _time.sleep(1)

    return Response(gen(), mimetype='text/event-stream')


@app.route('/api/strategy/multi_backtest/result')
def api_multi_backtest_result():
    """获取多策略回测结果"""
    run_id = request.args.get('run_id')
    if not run_id:
        return jsonify({'error': 'Missing run_id'}), 400
    res = MULTI_BT_RESULTS.get(run_id)
    if res is None:
        prog = MULTI_BT_PROGRESS.get(run_id)
        if prog:
            return jsonify({'status': 'running', 'progress': prog}), 202
        return jsonify({'error': 'run_id not found'}), 404
    return jsonify(res)


# ============== 启动函数 ==============

def run_web_server(host=None, port=None, debug=None):
    """
    启动Web服务器
    
    Args:
        host: 主机地址
        port: 端口
        debug: 是否调试模式
    """
    # 加载系统状态
    load_system_state()
    
    # 加载策略
    load_strategies_from_file()
    
    web_config = config.get_web_config()
    
    host = host or web_config['host']
    port = port or web_config['port']
    debug = debug if debug is not None else web_config['debug']
    
    logger.info(f"启动Web服务器: http://{host}:{port}")
    
    # 自动启动调度器（仅在真正的 worker 进程中启动，防止 Werkzeug reloader 的 watcher 进程重复启动）
    try:
        import os as _os
        _is_reloader_watcher = debug and _os.environ.get('WERKZEUG_RUN_MAIN') != 'true'
        if not _is_reloader_watcher and not scheduler.is_running:
            scheduler.start()
            logger.info("调度器已在Web进程中自动启动")
    except Exception as e:
        logger.warning(f"自动启动调度器失败: {e}")

    # 启动时立即拉取一次实时数据（初始化快照），后续由全局更新循环按交易时间维护
    import threading as _threading
    def _initial_realtime_fetch():
        global realtime_snapshot
        try:
            logger.info("启动时初始化实时数据...")
            realtime_data_df = unified_data.get_realtime_data(adjust=True)
            if not realtime_data_df.empty:
                realtime_data = {row.get('code', ''): row for _, row in realtime_data_df.iterrows()}
                realtime_snapshot.update(realtime_data)
                unified_data.merge_realtime_data(realtime_data)
                logger.info(f"初始实时数据已加载: {len(realtime_data)} 条")
        except Exception as e:
            logger.warning(f"初始实时数据加载失败: {e}")
    _threading.Thread(target=_initial_realtime_fetch, daemon=True).start()

    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_web_server()
