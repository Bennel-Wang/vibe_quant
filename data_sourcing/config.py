"""
统一量化数据源系统 - 配置模块
自动从主项目 config.yaml 读取 token 和日期等配置
"""
import os
import logging
from pathlib import Path

# === 项目路径 ===
PROJECT_ROOT = Path(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = PROJECT_ROOT.parent  # 统一使用仓库根目录管理数据和日志
DATA_DIR = _REPO_ROOT / "data" / "history"
LOG_DIR = _REPO_ROOT / "logs"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# === 读取主项目 config.yaml ===
_main_config = {}
_config_path = _REPO_ROOT / "config.yaml"
if _config_path.exists():
    try:
        import yaml
        with open(_config_path, 'r', encoding='utf-8') as _f:
            _main_config = yaml.safe_load(_f) or {}
    except Exception:
        pass

# === Tushare 配置（优先级：环境变量 > config.yaml > 空字符串）===
TUSHARE_TOKEN = (
    os.environ.get("TUSHARE_TOKEN", "")
    or _main_config.get("tokens", {}).get("tushare_token", "")
)

# === 历史数据起始日期（与主项目保持一致）===
DEFAULT_START_DATE = (
    _main_config.get("data_collection", {}).get("history", {}).get("start_date", "20030101")
)

# === 数据源优先级 ===
HISTORICAL_SOURCES = ["tushare", "baostock"]
REALTIME_SOURCES = ["easyquotation", "pytdx", "mootdx"]

# === 统一CSV列 ===
BASE_COLUMNS = [
    "uniformed_stock_code", "trade_date", "open", "high", "low", "close", "vol",
    "pe_ttm", "pb"
]

FULL_COLUMNS = [
    "uniformed_stock_code", "trade_date", "open", "high", "low", "close", "vol",
    "pe_ttm", "pb",
    "change", "pct_chg", "amount", "date", "volume",
    "rsi_6", "rsi_12", "rsi_24",
    "ma_5", "ma_20", "ma_60",
    "macd", "macd_signal", "macd_histogram",
    "boll_middle", "boll_upper", "boll_lower",
    "kdj_k", "kdj_d", "kdj_j",
    "volatility", "volume_ma_5", "volume_ma_20", "volume_ratio", "wr_14",
    "rsi6_pct100", "pettm_pct10y"
]

# === 测试样本 ===
TEST_SAMPLES = {
    "A股大盘": "000001.SH",
    "港股大盘": "HSI.HK",
    "A股板块": "399006.SZ",
    "A股个股": "000001.SZ",
    "港股个股": "00700.HK",
}

# === pytdx 服务器 ===
PYTDX_HQ_HOSTS = [
    ("119.147.212.81", 7709),
    ("112.74.214.43", 7727),
    ("221.231.141.60", 7709),
    ("101.227.73.20", 7709),
    ("101.227.77.254", 7709),
    ("14.215.128.18", 7709),
    ("59.173.18.140", 7709),
    ("218.75.126.9", 7709),
]

PYTDX_EX_HOSTS = [
    ("180.153.18.176", 7721),
    ("180.153.18.170", 7721),
    ("180.153.18.171", 7721),
    ("106.14.95.149", 7727),
    ("112.74.214.43", 7727),
    ("120.24.0.77", 7727),
    ("47.107.75.159", 7727),
    ("113.105.142.162", 7721),
]

# === 日志配置 ===
def setup_logger(name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
    """创建带文件和控制台输出的logger"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 控制台
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # 文件
    if log_file:
        fh = logging.FileHandler(
            LOG_DIR / log_file, encoding="utf-8", mode="a"
        )
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
