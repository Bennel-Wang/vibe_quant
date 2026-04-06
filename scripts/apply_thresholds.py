"""Apply extreme/lenient threshold transforms to strategies.json."""
import json, copy, sys

with open('data/strategies.json', encoding='utf-8') as f:
    original = json.load(f)

with open('data/strategies_v3_backup.json', 'w', encoding='utf-8') as f:
    json.dump(original, f, ensure_ascii=False, indent=2)

# Exact condition-string replacements (applied once, no chaining)
REPLACEMENTS = {
    # ==== BUY: individual oscillators -> more extreme ====
    'w_rsi6_pct100 < 15 and idx_ret_20 < -5':          'w_rsi6_pct100 < 8 and idx_ret_20 < -4',
    'm_rsi6_pct100 < 20':                                'm_rsi6_pct100 < 12',
    'w_rsi6_pct100 < 22 and rsi6_pct100 < 12':          'w_rsi6_pct100 < 12 and rsi6_pct100 < 8',
    'idx_ret_60 < -15 and rsi6_pct100 < 20':            'idx_ret_60 < -10 and rsi6_pct100 < 14',
    'pettm_pct10y < 40':                                 'pettm_pct10y < 25',
    'pettm_pct10y < 50 and rsi_6 < 30':                 'pettm_pct10y < 35 and rsi_6 < 23',
    'pettm_pct10y < 45 and boll_position < 0.20':       'pettm_pct10y < 30 and boll_position < 0.12',
    'pettm_pct10y < 55 and rsi6_pct100 < 8':            'pettm_pct10y < 40 and rsi6_pct100 < 5',
    'macd_histogram > 0 and ma_5 > ma_20 and close > ma_60 and rel_strength_10 > 3':
        'macd_histogram > 0 and ma_5 > ma_20 and close > ma_60 and rel_strength_10 > 7',
    'w_rsi6_pct100 < 20':                                'w_rsi6_pct100 < 10',
    'w_rsi6_pct100 < 28 and rsi_6 < 32':                'w_rsi6_pct100 < 15 and rsi_6 < 25',
    'w_rsi6_pct100 < 25 and idx_ret_20 < -6':           'w_rsi6_pct100 < 12 and idx_ret_20 < -4',
    'w_rsi6_pct100 < 32 and rsi6_pct100 < 10 and volume_ratio > 1.4':
        'w_rsi6_pct100 < 18 and rsi6_pct100 < 7 and volume_ratio > 1.7',
    'boll_position < 0.05 and idx_ret_20 > -8':         'boll_position < 0.03 and idx_ret_20 > -10',
    'boll_position < 0.08 and rsi_6 < 25 and idx_ret_20 > -8':
        'boll_position < 0.05 and rsi_6 < 18 and idx_ret_20 > -10',
    'boll_position < 0.10 and rsi6_pct100 < 7 and idx_ret_20 > -10':
        'boll_position < 0.06 and rsi6_pct100 < 4 and idx_ret_20 > -12',
    'boll_position < 0.06 and volume_ratio > 2.0':      'boll_position < 0.04 and volume_ratio > 2.5',
    'rel_strength_20 > 5 and close > ma_20':             'rel_strength_20 > 10 and close > ma_20',
    'rel_strength_10 > 8 and close > ma_60 and volume_ratio > 1.1':
        'rel_strength_10 > 13 and close > ma_60 and volume_ratio > 1.4',
    'rsi_6 < 20 and idx_ret_20 > -8':                   'rsi_6 < 14 and idx_ret_20 > -10',
    'rsi_6 < 25 and w_rsi6_pct100 < 15 and idx_ret_20 > -8':
        'rsi_6 < 18 and w_rsi6_pct100 < 9 and idx_ret_20 > -10',
    'rsi6_pct100 < 6 and idx_ret_20 > -10':             'rsi6_pct100 < 4 and idx_ret_20 > -12',
    'rsi_6 < 22 and boll_position < 0.15 and idx_ret_20 > -6':
        'rsi_6 < 15 and boll_position < 0.09 and idx_ret_20 > -8',
    'close > ma_60 and ma_20 > ma_60 and rsi_6 < 60':   'close > ma_60 and ma_20 > ma_60 and rsi_6 < 55',
    'close > ma_20 and close < ma_20 * 1.03 and ma_20 > ma_60 and rsi_6 < 55':
        'close > ma_20 and close < ma_20 * 1.02 and ma_20 > ma_60 and rsi_6 < 50',
    'pettm_pct10y < 30 and macd_histogram > 0 and close > ma_20':
        'pettm_pct10y < 20 and macd_histogram > 0 and close > ma_20',
    'pettm_pct10y < 25 and w_rsi6_pct100 < 32':         'pettm_pct10y < 15 and w_rsi6_pct100 < 20',
    'pettm_pct10y < 35 and rel_strength_10 > 0 and rsi6_pct100 > 25':
        'pettm_pct10y < 22 and rel_strength_10 > 4',
    'pettm_pct10y < 40 and ma_5 > ma_20 and volume_ratio > 1.3':
        'pettm_pct10y < 28 and ma_5 > ma_20 and volume_ratio > 1.6',

    # ==== EXCLUSION: market -> lenient (trigger at smaller values); PE/RSI -> stricter ====
    'idx_ret_60 > 8':       'idx_ret_60 > 12',
    'pettm_pct10y > 70':    'pettm_pct10y > 65',
    'rsi_6 > 82':           'rsi_6 > 88',
    'rsi_6 > 85':           'rsi_6 > 90',
    'm_rsi6_pct100 < 5':    'm_rsi6_pct100 < 3',
    'pettm_pct10y > 80':    'pettm_pct10y > 72',
    'idx_ret_20 < -12':     'idx_ret_20 < -15',
    'pettm_pct10y > 60':    'pettm_pct10y > 55',

    # ==== SELL: TP -> more extreme high; SL -> more extreme weakness ====
    'w_rsi6_pct100 > 75':                           'w_rsi6_pct100 > 83',
    'm_rsi6_pct100 > 85':                           'm_rsi6_pct100 > 90',
    'm_rsi6_pct100 > 92':                           'm_rsi6_pct100 > 95',
    'rsi_6 > 82 and w_rsi6_pct100 > 65':           'rsi_6 > 88 and w_rsi6_pct100 > 75',
    'pettm_pct10y > 78 and macd_histogram < 0':     'pettm_pct10y > 85 and macd_histogram < 0',
    'pettm_pct10y > 85':                            'pettm_pct10y > 90',
    'm_rsi6_pct100 > 88':                           'm_rsi6_pct100 > 93',
    'close < ma_60 * 0.93 and pettm_pct10y > 30':  'close < ma_60 * 0.91 and pettm_pct10y > 30',
    'rsi_6 > 85 and macd_histogram > 0':            'rsi_6 > 90 and macd_histogram > 0',
    'w_rsi6_pct100 > 70':                           'w_rsi6_pct100 > 80',
    'w_rsi6_pct100 > 60 and rsi_6 > 68':           'w_rsi6_pct100 > 72 and rsi_6 > 74',
    'w_rsi6_pct100 > 80':                           'w_rsi6_pct100 > 88',
    'boll_position > 0.85':                         'boll_position > 0.91',
    'rsi_6 > 72':                                   'rsi_6 > 78',
    'boll_position > 0.75 and rsi_6 > 65':         'boll_position > 0.83 and rsi_6 > 72',
    'rel_strength_5 < -7 and close < ma_20 * 0.96': 'rel_strength_5 < -10 and close < ma_20 * 0.96',
    'rel_strength_20 < -5 and macd_histogram < 0':  'rel_strength_20 < -9 and macd_histogram < 0',
    'rsi_6 > 65':                                   'rsi_6 > 70',
    'rsi_6 > 78':                                   'rsi_6 > 83',
    'boll_position > 0.82 and rsi_6 > 60':         'boll_position > 0.88 and rsi_6 > 66',
    'rsi_6 > 75':                                   'rsi_6 > 81',
    'pettm_pct10y > 72 and macd_histogram < 0':     'pettm_pct10y > 80 and macd_histogram < 0',
    'pettm_pct10y > 82':                            'pettm_pct10y > 88',
}

d = copy.deepcopy(original)
changed = 0
diffs = []
for strat_name, strat in d.items():
    for rule in strat.get('rules', []) + strat.get('exclusion_rules', []):
        cond = rule.get('condition', '')
        if cond in REPLACEMENTS:
            new_cond = REPLACEMENTS[cond]
            diffs.append((strat_name, cond, new_cond))
            rule['condition'] = new_cond
            changed += 1

print(f'Applied {changed} condition replacements:\n')
for sname, old, new in diffs:
    print(f'  [{sname}]')
    print(f'    - {old}')
    print(f'    + {new}')

with open('data/strategies.json', 'w', encoding='utf-8') as f:
    json.dump(d, f, ensure_ascii=False, indent=2)
with open('data/strategies_v2.json', 'w', encoding='utf-8') as f:
    json.dump(d, f, ensure_ascii=False, indent=2)

print('\nSaved strategies.json and strategies_v2.json')
