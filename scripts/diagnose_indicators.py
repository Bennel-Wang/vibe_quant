"""Diagnose what indicators are available for backtests."""
import sys
sys.path.insert(0, '.')
from quant_system.indicators import technical_indicators
from quant_system.backtest import BacktestEngine

df = technical_indicators.calculate_all_indicators('600519', '20220101', '20241231')
print('Columns:', list(df.columns))
print('Shape:', df.shape)

last = df.iloc[-1].to_dict()
key_cols = ['rsi_6', 'rsi6_pct100', 'macd_histogram', 'volume_ratio',
            'overall_score', 'boll_position', 'pct_chg', 'ma_5', 'ma_20', 'close']
print('\nKey indicator values (last row):')
for col in key_cols:
    val = last.get(col, 'MISSING')
    print(f'  {col}: {val}')

be = BacktestEngine()
df2 = be._merge_weekly_monthly('600519', df)
wm_cols = [c for c in df2.columns if c.startswith('w_') or c.startswith('m_')]
print('\nW/M cols available:', wm_cols)

last2 = df2.iloc[-1].to_dict()
wm_key = ['w_rsi6_pct100', 'm_rsi6_pct100', 'w_rsi_6', 'm_rsi_6', 
          'w_ma_5', 'w_ma_20', 'rel_strength_5', 'rel_strength_20', 
          'idx_ret_20', 'idx_ret_60']
print('\nW/M + rel values (last row):')
for col in wm_key:
    val = last2.get(col, 'MISSING')
    print(f'  {col}: {val}')

# Check data ranges for key strategy conditions
print('\n--- Condition feasibility check ---')
for col in ['w_rsi6_pct100', 'm_rsi6_pct100', 'rsi_6', 'volume_ratio', 
            'overall_score', 'boll_position', 'rel_strength_5', 'rel_strength_20',
            'idx_ret_20', 'idx_ret_60', 'news_sentiment']:
    if col in df2.columns:
        series = df2[col].dropna()
        if len(series) > 0:
            print(f'  {col}: min={series.min():.2f} max={series.max():.2f} '
                  f'mean={series.mean():.2f} nulls={df2[col].isna().sum()}')
        else:
            print(f'  {col}: ALL NULL')
    else:
        print(f'  {col}: NOT IN DATAFRAME')

# Test specific conditions from new strategies
print('\n--- Condition trigger test (2022-2024) ---')
import pandas as pd

df3 = df2.copy()
for col in ['news_sentiment', 'rel_strength_5', 'rel_strength_20', 'idx_ret_20', 'idx_ret_60']:
    if col not in df3.columns:
        df3[col] = 0.0

conditions = {
    '短线买入1': "volume_ratio > 1.5 and close > ma_5 and overall_score > -20",
    '短线买入2': "rsi_6 < 70 and boll_position > 0.5 and close > ma_20 and volume_ratio > 1.2",
    '短线卖出1': "close < ma_5",
    '中线1买入1': "rel_strength_20 > 5 and rsi_6 < 70 and close > ma_20",
    '中线2买入1': "w_rsi6_pct100 < 10 and rel_strength_5 > 0 and rsi_6 < 40",
    '中线2买入2': "w_rsi6_pct100 < 15 and close > ma_20 * 0.97 and rsi_6 < 35",
    '长线买入1': "m_rsi6_pct100 < 10 and w_rsi6_pct100 < 10 and idx_ret_20 < -8",
    '长线买入2': "m_rsi6_pct100 < 15 and pettm_pct10y < 15 and w_rsi_6 < 35",
    '超长线买入1': "w_rsi6_pct100 < 15 and pettm_pct10y < 20 and overall_score > -60",
}

for name, cond in conditions.items():
    try:
        mask = df3.eval(cond)
        count = mask.sum()
        pct = count / len(df3) * 100
        print(f'  {name}: {count} days triggered ({pct:.1f}%)')
    except Exception as e:
        print(f'  {name}: ERROR - {e}')
