"""
股票代码管理模块
统一管理股票、板块、大盘指数的代码
"""

import os
import yaml
import logging
from typing import List, Dict, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum

try:
    from quant_system.code_converter import to_unified_code as _to_unified_code
except ImportError:
    try:
        from code_converter import to_unified_code as _to_unified_code
    except ImportError:
        _to_unified_code = None

logger = logging.getLogger(__name__)


class StockType(Enum):
    """股票类型"""
    STOCK = "stock"      # 个股
    ETF = "etf"          # ETF基金（如 510300.SH、159919.SZ）
    SECTOR = "sector"    # 板块
    INDEX = "index"      # 大盘指数


@dataclass
class StockInfo:
    """股票信息数据类"""
    name: str           # 股票名称
    code: str           # 股票代码
    market: str = ""    # 市场 (sh/sz)
    type: str = "stock" # 类型
    notes: str = ""     # 备注
    industry: str = ""  # 所属板块
    strategy: str = ""  # 分配的策略名称（向后兼容）
    buy_strategy: str = ""   # 独立买入策略名称
    sell_strategy: str = ""  # 独立卖出策略名称
    list_date: str = "" # 上市日期 YYYYMMDD（可选，用于数据完整性校验）
    
    def __post_init__(self):
        """初始化后处理"""
        if self.type == "index" and not self.market:
            # 根据代码判断指数市场
            if self.code.startswith('0') or self.code.startswith('3'):
                self.market = "sz"
            else:
                self.market = "sh"
    
    @property
    def full_code(self) -> str:
        """获取完整代码（统一格式: 代码.后缀），如 000001.SH, 600519.SH, 09988.HK"""
        return self.unified_code

    @property
    def prefix_code(self) -> str:
        """获取带市场前缀的代码（用于easyquotation等API），如 sh600519"""
        if self.type == "sector":
            return self.code
        if self.market and self.code.lower().startswith(self.market.lower()):
            return self.code
        return f"{self.market}{self.code}"

    @property
    def storage_code(self) -> str:
        """获取用于文件存储的代码（带市场前缀），如 sh600519"""
        return self.prefix_code
    
    @property
    def tushare_code(self) -> str:
        """获取Tushare格式的代码，如 600519.SH"""
        if self.type == "sector":
            # 板块代码处理
            if self.code.startswith('BK'):
                # 旧版板块代码，如 BK1036
                return self.code
            elif self.code.isdigit() and len(self.code) == 6:
                # 申万行业代码，如 801010
                return f"{self.code}.SI"
            else:
                return self.code
        
        # 特殊处理主要指数
        index_sh = ['000001', '000300', '000016', '000010', '000009', '000002', '000003', '000688']
        index_sz = ['399001', '399006', '399005', '399106', '399107', '399108']
        
        if self.type == "index":
            # 指数类型，根据市场添加后缀
            if self.code in index_sh:
                return f"{self.code}.SH"
            elif self.code in index_sz:
                return f"{self.code}.SZ"
            else:
                # 默认使用市场信息
                market_upper = self.market.upper()
                return f"{self.code}.{market_upper}"
        
        # 个股类型
        market_upper = self.market.upper()
        return f"{self.code}.{market_upper}"
    
    @property
    def easyquotation_code(self) -> str:
        """获取Easyquotation格式的代码，如 sh600519"""
        return self.prefix_code

    @property
    def unified_code(self) -> str:
        """获取data_sourcing统一格式的代码，如 600519.SH, 00700.HK, HSI.HK"""
        if self.market == "hk":
            # 港股: hkHSI → HSI.HK, hk00700 → 00700.HK, 09988 → 09988.HK
            pure = self.code
            if pure.lower().startswith("hk"):
                pure = pure[2:]
            return f"{pure}.HK"
        elif self.market == "sh":
            return f"{self.code}.SH"
        elif self.market == "sz":
            return f"{self.code}.SZ"
        elif self.type == "sector":
            # 板块: 尝试判断市场
            if self.code.startswith("399"):
                return f"{self.code}.SZ"
            return f"{self.code}.SH"
        else:
            return f"{self.code}.SH"

    def to_dict(self) -> Dict:
        """转换为字典（空备注和空板块不输出，保持YAML简洁）"""
        d = asdict(self)
        if not d.get('notes'):
            d.pop('notes', None)
        if not d.get('industry'):
            d.pop('industry', None)
        if not d.get('strategy'):
            d.pop('strategy', None)
        if not d.get('buy_strategy'):
            d.pop('buy_strategy', None)
        if not d.get('sell_strategy'):
            d.pop('sell_strategy', None)
        if not d.get('list_date'):
            d.pop('list_date', None)
        return d


class StockManager:
    """股票代码管理器"""
    
    def __init__(self, config_path: str = "config/stocks.yaml"):
        self.config_path = config_path
        self.stocks: List[StockInfo] = []
        self.sectors: List[StockInfo] = []
        self.indices: List[StockInfo] = []
        self._load_stocks()
    
    def _load_stocks(self):
        """从配置文件加载股票代码"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"股票配置文件不存在: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        # 加载个股
        if 'stocks' in data:
            for item in data['stocks']:
                self.stocks.append(StockInfo(**item))
        
        # 加载板块
        if 'sectors' in data:
            for item in data['sectors']:
                self.sectors.append(StockInfo(**item))
        
        # 加载指数
        if 'indices' in data:
            for item in data['indices']:
                self.indices.append(StockInfo(**item))
    
    def get_all_stocks(self) -> List[StockInfo]:
        """获取所有股票（包括个股、板块、指数）"""
        return self.stocks + self.sectors + self.indices
    
    def get_stocks(self) -> List[StockInfo]:
        """获取所有个股"""
        return self.stocks.copy()
    
    def get_sectors(self) -> List[StockInfo]:
        """获取所有板块"""
        return self.sectors.copy()
    
    def get_indices(self) -> List[StockInfo]:
        """获取所有指数"""
        return self.indices.copy()
    
    def get_stock_by_code(self, code: str) -> Optional[StockInfo]:
        """
        根据股票代码获取股票信息（仅支持后缀格式）
        
        Args:
            code: 股票代码，后缀格式如 600519.SH、000001.SZ、HSI.HK
        
        Returns:
            StockInfo对象或None
        """
        if '.' not in code:
            logger.warning(f"get_stock_by_code 建议使用后缀格式(如 600519.SH)，收到: {code}")
            # 尝试自动转换为后缀格式
            if _to_unified_code is not None:
                converted = _to_unified_code(code)
                if '.' in converted:
                    logger.info(f"get_stock_by_code 自动转换: {code} → {converted}")
                    return self.get_stock_by_code(converted)
            # 兜底：裸代码模糊匹配
            for stock in self.get_all_stocks():
                if stock.code == code:
                    return stock
            return None
        
        # 后缀格式匹配：000001.SH → code='000001', market='sh'
        parts = code.split('.')
        bare_code = parts[0]
        market = parts[1].lower() if len(parts) > 1 else ''
        
        for stock in self.get_all_stocks():
            if stock.code == bare_code and stock.market == market:
                return stock
        
        return None
    
    def get_stock_by_name(self, name: str) -> Optional[StockInfo]:
        """
        根据名称获取股票信息
        
        Args:
            name: 股票名称
        
        Returns:
            StockInfo对象或None
        """
        for stock in self.get_all_stocks():
            if stock.name == name:
                return stock
        
        return None
    
    def _normalize_code(self, code: str) -> str:
        """
        标准化股票代码
        
        Args:
            code: 原始代码
        
        Returns:
            纯数字代码
        """
        # 移除市场前缀（sh/sz/hk）
        if code.startswith(('sh', 'sz', 'hk')):
            code = code[2:]
        
        # 移除市场后缀
        if '.' in code:
            code = code.split('.')[0]
        
        # 移除 HSI 等特殊代码的 hk 前缀
        if code == 'HSI':
            pass  # HSI stays as HSI
        
        return code
    
    def add_stock(self, name: str, code: str, market: str = "", 
                  stock_type: str = "stock", industry: str = "", save: bool = True):
        """
        添加股票

        Args:
            name: 股票名称
            code: 股票代码
            market: 市场 (sh/sz/hk)
            stock_type: 类型 (stock/etf/sector/index)
            industry: 所属板块
            save: 是否保存到配置文件
        """
        stock = StockInfo(name=name, code=code, market=market, type=stock_type, industry=industry)
        
        if stock_type in ("stock", "etf"):  # ETF 与个股存储在同一列表，仅 type 字段区分
            self.stocks.append(stock)
        elif stock_type == "sector":
            self.sectors.append(stock)
        elif stock_type == "index":
            self.indices.append(stock)
        
        if save:
            self.save()
    
    def remove_stock(self, code: str, save: bool = True):
        """
        移除股票（精确匹配市场，避免误删同代码不同市场的条目）
        
        Args:
            code: 股票代码，后缀格式如 000001.SZ
            save: 是否保存到配置文件
        """
        target = self.get_stock_by_code(code)
        if target:
            self.stocks = [s for s in self.stocks if not (s.code == target.code and s.market == target.market)]
            self.sectors = [s for s in self.sectors if not (s.code == target.code and s.market == target.market)]
            self.indices = [s for s in self.indices if not (s.code == target.code and s.market == target.market)]
        
        if save:
            self.save()
    
    def save(self):
        """保存到配置文件"""
        data = {
            'indices': [s.to_dict() for s in self.indices],
            'sectors': [s.to_dict() for s in self.sectors],
            'stocks': [s.to_dict() for s in self.stocks],
        }
        
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    
    def get_tushare_codes(self, stock_type: Optional[StockType] = None) -> List[str]:
        """
        获取Tushare格式的代码列表
        
        Args:
            stock_type: 股票类型筛选
        
        Returns:
            代码列表
        """
        stocks = self._get_stocks_by_type(stock_type)
        return [s.tushare_code for s in stocks if s.type != "sector"]
    
    def get_easyquotation_codes(self, stock_type: Optional[StockType] = None) -> List[str]:
        """
        获取Easyquotation格式的代码列表
        
        Args:
            stock_type: 股票类型筛选
        
        Returns:
            代码列表
        """
        stocks = self._get_stocks_by_type(stock_type)
        return [s.easyquotation_code for s in stocks]
    
    def _get_stocks_by_type(self, stock_type: Optional[StockType] = None) -> List[StockInfo]:
        """根据类型获取股票列表"""
        if stock_type is None:
            return self.get_all_stocks()
        
        if stock_type == StockType.STOCK:
            return self.stocks
        elif stock_type == StockType.SECTOR:
            return self.sectors
        elif stock_type == StockType.INDEX:
            return self.indices
        
        return []
    
    def to_dataframe(self):
        """转换为DataFrame"""
        import pandas as pd
        
        data = []
        for stock in self.get_all_stocks():
            data.append({
                'name': stock.name,
                'code': stock.code,
                'market': stock.market,
                'type': stock.type,
                'full_code': stock.full_code,
                'tushare_code': stock.tushare_code,
            })
        
        return pd.DataFrame(data)


# 全局股票管理器实例
stock_manager = StockManager()
