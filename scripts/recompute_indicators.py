import logging
from datetime import datetime, timedelta
import pandas as pd
import sys, os
# ensure project root is on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from quant_system.indicators import technical_indicators, fresh_technical_indicators
from quant_system.stock_manager import stock_manager
from quant_system.data_source import unified_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('recompute_indicators')

start_date = (datetime.now() - timedelta(days=365*6)).strftime('%Y%m%d')
end_date = datetime.now().strftime('%Y%m%d')

stocks = stock_manager.get_all_stocks()
logger.info(f"Found {len(stocks)} stocks to recompute indicators for")

for s in stocks:
    code = s.full_code if hasattr(s, 'full_code') and s.full_code else s.code
    try:
        logger.info(f"Processing {code}...")
        # fetch adjusted daily data
        df = unified_data.get_historical_data(code, start_date, end_date, freq='day', adjust=True)
        if df.empty:
            logger.warning(f"No data for {code}, skipping")
            continue
        # normalize date
        if df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d', errors='coerce')
        else:
            df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        # daily indicators using fresh calculator
        try:
            ind_day = fresh_technical_indicators.calculate_fresh_indicators(code, start_date, end_date, freq='day')
            if not ind_day.empty:
                technical_indicators.save_indicators(code, ind_day, freq='day')
                logger.info(f"Saved day indicators for {code}, rows={len(ind_day)}")
        except Exception as e:
            logger.exception(f"Fresh day indicators failed for {code}: {e}")

        # weekly resample from daily price data
        try:
            df2 = df.copy()
            df2.set_index('date', inplace=True)
            weekly = df2.resample('W').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna().reset_index()
            if not weekly.empty:
                ind_week = technical_indicators.calculate_all_indicators_from_df(weekly)
                technical_indicators.save_indicators(code, ind_week, freq='week')
                logger.info(f"Saved week indicators for {code}, rows={len(ind_week)}")
        except Exception as e:
            logger.exception(f"Week indicators failed for {code}: {e}")

        # monthly resample
        try:
            df3 = df.copy()
            df3.set_index('date', inplace=True)
            monthly = df3.resample('M').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna().reset_index()
            if not monthly.empty:
                ind_month = technical_indicators.calculate_all_indicators_from_df(monthly)
                technical_indicators.save_indicators(code, ind_month, freq='month')
                logger.info(f"Saved month indicators for {code}, rows={len(ind_month)}")
        except Exception as e:
            logger.exception(f"Month indicators failed for {code}: {e}")

    except Exception as e:
        logger.exception(f"Failed processing {code}: {e}")

logger.info("Recompute finished")
