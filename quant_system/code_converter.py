"""
股票代码转换工具
参考 quant_stock_program/utils/historical_data_fetch.py 和 news_fetch.py 实现
"""

import logging
from typing import Union

logger = logging.getLogger(__name__)


def convert_stock_code(code: str) -> str:
    """
    将股票代码转换为不同系统的标准格式
    
    Args:
        code: 原始股票代码，如 'sh600111', '000001', 'BK1186', 'hk00700'
    
    Returns:
        转换后的代码
    
    Examples:
        >>> convert_stock_code('sh600519')  # 新浪财经格式
        'sh600519'
        >>> convert_stock_code('000001')    # Tushare格式
        '000001.SZ'
        >>> convert_stock_code('BK1186')    # 板块代码
        'BK1186'
        >>> convert_stock_code('hk00700')   # 港股代码
        '00700.HK'
    """
    
    # 板块代码处理
    if code.startswith('BK'):
        return code
    
    # 港股代码处理
    if code.startswith('hk') or code.startswith('HK'):
        # hk00700 -> 00700.HK
        number = code[2:] if code.startswith('hk') else code[2:]
        return f"{number}.HK"
    elif len(code) == 5 and code.isdigit():
        # 5位数字代码认为是港股 (如 09988)
        return f"{code}.HK"
    
    # 指数代码特殊处理
    if code in ['hkHSI']:
        return 'HSI'  # 恒生指数特殊代码
    
    # A股代码处理
    if code.startswith('sh') or code.startswith('sz'):
        # sh600519 -> sh600519 (新浪财经格式)
        return code
    else:
        # 000001 -> 000001.SH, 600519 -> 600519.SH
        # 根据代码前缀判断市场
        if code.startswith('000'):  # 上证指数系列
            return f"{code}.SH"
        elif code.startswith(('0', '3')):  # 深证系列
            return f"{code}.SZ"
        elif code.startswith(('6', '5')):  # 上证股票系列
            return f"{code}.SH"
        else:
            # 默认认为是上海交易所
            return f"{code}.SH"


def get_tushare_code(code: str) -> str:
    """
    获取Tushare格式的代码
    
    Args:
        code: 原始代码
        
    Returns:
        Tushare格式代码 (如 600519.SH)
    """
    # 板块代码特殊处理
    if is_sector_code(code):
        if code.endswith('.SI'):
            return code  # 已经是正确格式
        elif len(code) == 6 and code.startswith('801'):
            return f"{code}.SI"  # 转换为申万行业格式
        else:
            return code  # 其他板块代码保持原样
    
    converted = convert_stock_code(code)
    
    # 如果已经是Tushare格式，直接返回
    if '.' in converted and converted.split('.')[1] in ['SH', 'SZ', 'HK']:
        return converted
    
    # 否则转换为Tushare格式
    if converted.startswith('sh'):
        return f"{converted[2:]}.SH"
    elif converted.startswith('sz'):
        return f"{converted[2:]}.SZ"
    elif converted.startswith('BK'):
        return converted  # 板块代码保持不变
    elif converted.endswith('.HK'):
        return converted  # 港股代码保持不变
    else:
        # 纯数字代码
        if code.startswith(('0', '3')):
            return f"{code}.SZ"
        else:
            return f"{code}.SH"


def get_sina_code(code: str) -> str:
    """
    获取新浪财经格式的代码
    
    Args:
        code: 原始代码
        
    Returns:
        新浪财经格式代码 (如 sh600519)
    """
    converted = convert_stock_code(code)
    
    # 如果已经是新浪财经格式，直接返回
    if converted.startswith(('sh', 'sz')):
        return converted
    
    # 转换为新浪财经格式
    if converted.endswith('.SH'):
        return f"sh{converted[:-3]}"
    elif converted.endswith('.SZ'):
        return f"sz{converted[:-3]}"
    elif converted.endswith('.HK'):
        return f"hk{converted[:-3]}"
    elif converted.startswith('BK'):
        return converted
    elif converted == 'HSI':
        return 'hkHSI'
    else:
        # 默认处理
        if converted.startswith(('0', '3')):
            return f"sz{converted}"
        else:
            return f"sh{converted}"


def get_full_code(code: str) -> str:
    """
    获取完整代码 (兼容旧版接口)
    
    Args:
        code: 原始代码
        
    Returns:
        完整代码
    """
    return get_sina_code(code)


def is_sector_code(code: str) -> bool:
    """
    判断是否为板块代码
    
    Args:
        code: 股票代码
        
    Returns:
        是否为板块代码
    """
    # 支持多种板块代码格式
    sector_patterns = [
        lambda x: x.startswith('BK'),           # 旧版板块代码
        lambda x: x.endswith('.SI'),            # 申万行业代码
        lambda x: len(x) == 6 and x.startswith('801')  # 申万行业代码纯数字格式
    ]
    
    return any(pattern(code) for pattern in sector_patterns)


def is_index_code(code: str) -> bool:
    """
    判断是否为指数代码
    
    Args:
        code: 股票代码
        
    Returns:
        是否为指数代码
    """
    # 主要指数代码
    major_indices = [
        '000001', '000300', '000016', '000010', '000009', '000002', '000003', '000688',  # 上证指数
        '399001', '399006', '399005', '399106', '399107', '399108',  # 深证指数
        'HSI', 'hkHSI'  # 恒生指数
    ]
    
    # 板块指数（也归类为指数）
    if is_sector_code(code):
        return True
    
    # 主要指数
    if code in major_indices:
        return True
    
    # Tushare格式指数
    if code.endswith(('.SH', '.SZ', '.HK')) and code.split('.')[0] in major_indices:
        return True
        
    return False


def get_market_from_code(code: str) -> str:
    """
    从代码推断市场
    
    Args:
        code: 股票代码
        
    Returns:
        市场标识 (sh/sz/hk)
    """
    if code.startswith('sh') or code.endswith('.SH'):
        return 'sh'
    elif code.startswith('sz') or code.endswith('.SZ'):
        return 'sz'
    elif code.startswith('hk') or code.endswith('.HK') or code == 'HSI':
        return 'hk'
    elif code.startswith('BK'):
        return 'sector'
    else:
        # 根据代码前缀判断
        pure_code = code.split('.')[0] if '.' in code else code
        # 上证指数系列 (000开头) 属于上海交易所
        if pure_code.startswith('000'):
            return 'sh'
        # 深证指数系列 (399开头) 属于深圳交易所
        elif pure_code.startswith('399'):
            return 'sz'
        # 其他情况根据第一位数字判断
        elif pure_code.startswith(('0', '3')):
            return 'sz'
        else:
            return 'sh'


def to_unified_code(code: str) -> str:
    """
    将各种格式的股票代码转换为data_sourcing统一格式
    
    Args:
        code: 原始代码，如 'sh600519', 'sz000001', 'hk00700', 'hkHSI',
              '600519.SH', '000001.SZ', 'HSI.HK', 'BK1186', '801010'
    
    Returns:
        统一格式代码，如 '600519.SH', '000001.SZ', '00700.HK', 'HSI.HK'
    """
    # 已经是统一格式 (XXX.SH/SZ/HK)
    if '.' in code:
        parts = code.split('.')
        if parts[1].upper() in ('SH', 'SZ', 'HK', 'SI'):
            return code.upper()
    
    # 港股特殊代码
    if code == 'hkHSI' or code == 'HSI':
        return 'HSI.HK'
    if code.upper().startswith('HSCEI') or code == 'hkHSCEI':
        return 'HSCEI.HK'
    
    # hk前缀的港股
    if code.startswith('hk') or code.startswith('HK'):
        pure = code[2:]
        return f"{pure}.HK"
    
    # 5位纯数字视为港股
    if len(code) == 5 and code.isdigit():
        return f"{code}.HK"
    
    # sh/sz前缀的A股
    if code.startswith('sh'):
        return f"{code[2:]}.SH"
    if code.startswith('sz'):
        return f"{code[2:]}.SZ"
    
    # 板块代码
    if code.startswith('BK'):
        return code
    if len(code) == 6 and code.startswith('801'):
        return f"{code}.SI"
    
    # 纯数字A股代码
    if code.isdigit() and len(code) == 6:
        if code.startswith('000'):
            return f"{code}.SH"  # 上证指数系列
        elif code.startswith('399'):
            return f"{code}.SZ"  # 深证指数系列
        elif code.startswith(('0', '3')):
            return f"{code}.SZ"
        else:
            return f"{code}.SH"
    
    return code


def from_unified_code(unified: str) -> str:
    """
    将统一格式代码转回旧格式 (sh/sz/hk前缀)
    
    Args:
        unified: 统一格式代码，如 '600519.SH', 'HSI.HK'
    
    Returns:
        旧格式代码，如 'sh600519', 'hkHSI'
    """
    if '.' not in unified:
        return unified
    
    parts = unified.split('.')
    symbol = parts[0]
    suffix = parts[1].upper()
    
    if suffix == 'SH':
        return f"sh{symbol}"
    elif suffix == 'SZ':
        return f"sz{symbol}"
    elif suffix == 'HK':
        return f"hk{symbol}"
    
    return unified


# 测试函数
def test_code_conversion():
    """测试代码转换功能"""
    test_cases = [
        'sh600519',
        '000001',
        '399001',
        'BK1186',
        'hk00700',
        'hkHSI'
    ]
    
    print("代码转换测试:")
    print("-" * 50)
    
    for code in test_cases:
        print(f"原始代码: {code}")
        print(f"  Tushare格式: {get_tushare_code(code)}")
        print(f"  新浪格式: {get_sina_code(code)}")
        print(f"  是否板块: {is_sector_code(code)}")
        print(f"  是否指数: {is_index_code(code)}")
        print(f"  市场: {get_market_from_code(code)}")
        print()


if __name__ == "__main__":
    test_code_conversion()