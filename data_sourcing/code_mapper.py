"""
股票代码映射模块 - 统一代码格式转换为各数据源所需格式

统一代码格式:
  A股个股: 000001.SZ, 600000.SH
  A股指数: 000001.SH (上证指数), 399001.SZ (深证成指), 399006.SZ (创业板指)
  港股个股: 00700.HK
  港股指数: HSI.HK (恒生指数), HSCEI.HK (H股指数)
"""


def get_market(code: str) -> str:
    """判断市场类型: A_STOCK, A_INDEX, HK_STOCK, HK_INDEX, SW_INDEX"""
    symbol, suffix = _split_code(code)
    if suffix == "HK":
        if symbol.upper() in ("HSI", "HSCEI", "HSCCI", "HSTECH"):
            return "HK_INDEX"
        return "HK_STOCK"
    if suffix in ("SH", "SZ"):
        if _is_a_index(symbol, suffix):
            return "A_INDEX"
        if symbol.startswith("801"):
            return "SW_INDEX"
        return "A_STOCK"
    if suffix == "SI":
        return "SW_INDEX"
    return "UNKNOWN"


def _split_code(code: str) -> tuple:
    """分拆统一代码为 (symbol, suffix)"""
    parts = code.strip().upper().split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    return code.strip(), ""


def _is_a_index(symbol: str, suffix: str) -> bool:
    """判断是否为A股指数"""
    if suffix == "SH" and symbol.startswith("000"):
        return True
    if suffix == "SZ" and symbol.startswith("399"):
        return True
    return False


# === Tushare 格式 ===
def to_tushare(code: str) -> str:
    """统一代码 -> tushare格式 (000001.SZ, 000001.SH)"""
    symbol, suffix = _split_code(code)
    if suffix == "HK":
        return f"{symbol}.HK"
    return f"{symbol}.{suffix}"


# === Baostock 格式 ===
def to_baostock(code: str) -> str:
    """统一代码 -> baostock格式 (sz.000001, sh.600000)"""
    symbol, suffix = _split_code(code)
    if suffix == "SH":
        return f"sh.{symbol}"
    elif suffix == "SZ":
        return f"sz.{symbol}"
    return None  # baostock不支持港股


# === Easyquotation 格式 ===
def to_easyquotation(code: str) -> tuple:
    """统一代码 -> easyquotation格式
    返回 (code_str, source_type)
    A股: ('000001', 'sina')
    港股: ('00700', 'hkquote')
    """
    symbol, suffix = _split_code(code)
    market = get_market(code)
    if market.startswith("A_"):
        prefix = "sh" if suffix == "SH" else "sz"
        return f"{prefix}{symbol}", "sina"
    elif market.startswith("HK_"):
        return symbol, "hkquote"
    return None, None


# === Pytdx 格式 ===
def to_pytdx(code: str) -> tuple:
    """统一代码 -> pytdx格式
    A股: (market_code, symbol) - market: 0=SZ, 1=SH
    港股: (market_code, symbol) - 用扩展行情接口
    返回 (market, symbol, is_hk)
    """
    symbol, suffix = _split_code(code)
    market_type = get_market(code)
    if market_type in ("A_STOCK", "A_INDEX"):
        market = 1 if suffix == "SH" else 0
        return market, symbol, False
    elif market_type in ("HK_STOCK", "HK_INDEX"):
        return 31, symbol, True  # 港股市场代码
    return None, None, None


# === Mootdx 格式 ===
def to_mootdx(code: str) -> tuple:
    """统一代码 -> mootdx格式
    返回 (symbol, market_type)
    market_type: 'std' for A股, 'ext' for 港股
    """
    symbol, suffix = _split_code(code)
    market_type = get_market(code)
    if market_type in ("A_STOCK", "A_INDEX"):
        return symbol, "std"
    elif market_type in ("HK_STOCK", "HK_INDEX"):
        return symbol, "ext"
    return None, None


def from_tushare(ts_code: str) -> str:
    """tushare格式 -> 统一代码"""
    return ts_code.strip().upper()


def from_baostock(bs_code: str) -> str:
    """baostock格式 -> 统一代码 (sh.000001 -> 000001.SH)"""
    parts = bs_code.strip().split(".")
    if len(parts) == 2:
        prefix, symbol = parts
        suffix = "SH" if prefix.lower() == "sh" else "SZ"
        return f"{symbol}.{suffix}"
    return bs_code
