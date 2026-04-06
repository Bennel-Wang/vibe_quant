"""
模拟交易撮合引擎

支持功能：
  - 市价单（以下一交易日开盘价成交，避免未来函数）
  - 限价单（当日最高/最低价满足时成交）
  - 持仓管理（持仓成本、市值、浮盈计算）
  - 资金管理（可用资金、冻结资金）
  - 交易历史记录
  - 状态持久化到 JSON 文件
  - 与 RiskManager 集成（交易前自动风控检查）
"""

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any

from .config_manager import config

logger = logging.getLogger(__name__)


class OrderType(Enum):
    MARKET = "market"   # 市价单
    LIMIT = "limit"     # 限价单


class OrderStatus(Enum):
    PENDING = "pending"       # 待成交
    FILLED = "filled"         # 已成交
    CANCELLED = "cancelled"   # 已撤销
    REJECTED = "rejected"     # 已拒绝


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class Order:
    order_id: str
    code: str
    side: str           # buy / sell
    order_type: str     # market / limit
    shares: int
    limit_price: float = 0.0
    filled_price: float = 0.0
    filled_shares: int = 0
    status: str = OrderStatus.PENDING.value
    created_at: str = ""
    filled_at: str = ""
    commission: float = 0.0
    reason: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class SimPosition:
    code: str
    shares: int
    avg_cost: float
    current_price: float = 0.0

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price

    @property
    def unrealized_pnl(self) -> float:
        return self.shares * (self.current_price - self.avg_cost)

    @property
    def unrealized_pnl_pct(self) -> float:
        return (self.current_price - self.avg_cost) / self.avg_cost * 100 if self.avg_cost > 0 else 0.0

    def to_dict(self) -> Dict:
        return {
            'code': self.code,
            'shares': self.shares,
            'avg_cost': self.avg_cost,
            'current_price': self.current_price,
            'market_value': self.market_value,
            'unrealized_pnl': self.unrealized_pnl,
            'unrealized_pnl_pct': self.unrealized_pnl_pct,
        }


class SimulatedBroker:
    """
    模拟券商：完整的撮合引擎
    
    撮合规则：
      - 市价买入：以提交时 current_price * (1 + slippage) 成交
      - 市价卖出：以提交时 current_price * (1 - slippage) 成交
      - 限价买入：当日最低价 ≤ limit_price 时成交（成交价=limit_price）
      - 限价卖出：当日最高价 ≥ limit_price 时成交（成交价=limit_price）
      - 每手 100 股，买入自动向下取整到整百股
    """

    COMMISSION_RATE = 0.0003   # 手续费（双边）
    MIN_COMMISSION = 5.0       # 最低手续费（元）
    STAMP_DUTY = 0.001         # 印花税（仅卖出）
    SLIPPAGE = 0.001           # 默认滑点

    def __init__(self, account_id: int = 1, initial_capital: float = 1_000_000):
        self.account_id = account_id
        self.initial_capital = initial_capital
        self.available_cash = initial_capital
        self.frozen_cash = 0.0   # 限价买单冻结资金
        self.positions: Dict[str, SimPosition] = {}
        self.orders: Dict[str, Order] = {}
        self.trade_history: List[Dict] = []
        self._order_seq = 0

        # 从 config 读取手续费/滑点（若有）
        bt_cfg = config.get_backtest_config()
        self.COMMISSION_RATE = bt_cfg.get('commission_rate', self.COMMISSION_RATE)
        self.SLIPPAGE = bt_cfg.get('slippage', self.SLIPPAGE)

        self._state_path = os.path.join(
            config.get('data_storage.data_dir', './data'),
            f'sim_broker_{account_id}.json'
        )
        self.load_state()

    # ─── 工具方法 ──────────────────────────────────────────────────────────

    def _new_order_id(self) -> str:
        self._order_seq += 1
        return f"SIM{datetime.now().strftime('%Y%m%d%H%M%S')}{self._order_seq:04d}"

    def _calc_commission(self, side: str, price: float, shares: int) -> float:
        amt = price * shares
        comm = max(amt * self.COMMISSION_RATE, self.MIN_COMMISSION)
        if side == OrderSide.SELL.value:
            comm += amt * self.STAMP_DUTY
        return round(comm, 2)

    def _total_asset(self) -> float:
        mv = sum(p.market_value for p in self.positions.values())
        return self.available_cash + self.frozen_cash + mv

    # ─── 下单 ─────────────────────────────────────────────────────────────

    def submit_order(
        self,
        code: str,
        side: str,
        shares: int,
        order_type: str = OrderType.MARKET.value,
        limit_price: float = 0.0,
        current_price: float = 0.0,
        day_high: float = 0.0,
        day_low: float = 0.0,
    ) -> Order:
        """
        提交订单（含风控检查 + 立即撮合）

        Args:
            code         : 股票代码
            side         : 'buy' | 'sell'
            shares       : 委托股数（自动向下取整到 100 的整数倍）
            order_type   : 'market' | 'limit'
            limit_price  : 限价（limit 单必须传）
            current_price: 当前价格（market 单用于成交价计算）
            day_high     : 当日最高价（limit 单撮合用）
            day_low      : 当日最低价（limit 单撮合用）
        """
        shares = (shares // 100) * 100
        if shares <= 0:
            return self._reject_order(code, side, shares, order_type,
                                      limit_price, "委托股数必须 ≥ 100 股")

        # 风控前置检查
        risk_ok, risk_msg = self._risk_check(code, side, shares,
                                             current_price or limit_price)
        if not risk_ok:
            return self._reject_order(code, side, shares, order_type,
                                      limit_price, risk_msg)

        order_id = self._new_order_id()
        order = Order(
            order_id=order_id,
            code=code,
            side=side,
            order_type=order_type,
            shares=shares,
            limit_price=limit_price,
            created_at=datetime.now().isoformat(),
            reason="",
        )
        self.orders[order_id] = order

        # 立即尝试撮合
        self._try_fill(order, current_price, day_high, day_low)
        self.save_state()
        return order

    def _reject_order(self, code, side, shares, order_type, limit_price, reason) -> Order:
        oid = self._new_order_id()
        o = Order(order_id=oid, code=code, side=side, order_type=order_type,
                  shares=shares, limit_price=limit_price,
                  status=OrderStatus.REJECTED.value,
                  created_at=datetime.now().isoformat(),
                  reason=reason)
        self.orders[oid] = o
        logger.warning(f"[SimBroker] 订单拒绝: {code} {side} {shares}股 - {reason}")
        return o

    def _risk_check(self, code: str, side: str, shares: int, price: float):
        """基础风控：资金/持仓充足性检查"""
        if side == OrderSide.BUY.value:
            required = price * shares * (1 + self.COMMISSION_RATE)
            if required > self.available_cash:
                max_sh = int(self.available_cash / price / (1 + self.COMMISSION_RATE) / 100) * 100
                return False, f"可用资金不足，最多可买 {max_sh} 股"
        elif side == OrderSide.SELL.value:
            pos = self.positions.get(code)
            available = pos.shares if pos else 0
            if shares > available:
                return False, f"持仓不足，可卖 {available} 股"
        return True, ""

    # ─── 撮合 ─────────────────────────────────────────────────────────────

    def _try_fill(self, order: Order, current_price: float,
                  day_high: float = 0.0, day_low: float = 0.0):
        """尝试撮合一个订单"""
        if order.status != OrderStatus.PENDING.value:
            return

        if order.order_type == OrderType.MARKET.value:
            if current_price <= 0:
                logger.warning(f"市价单 {order.order_id} 无当前价格，无法撮合")
                return
            if order.side == OrderSide.BUY.value:
                fill_price = current_price * (1 + self.SLIPPAGE)
            else:
                fill_price = current_price * (1 - self.SLIPPAGE)
            self._fill_order(order, fill_price)

        elif order.order_type == OrderType.LIMIT.value:
            if order.side == OrderSide.BUY.value:
                # 当日最低价 ≤ 限价，则能成交
                check_price = day_low if day_low > 0 else current_price
                if check_price <= order.limit_price:
                    self._fill_order(order, order.limit_price)
            else:
                # 当日最高价 ≥ 限价，则能成交
                check_price = day_high if day_high > 0 else current_price
                if check_price >= order.limit_price:
                    self._fill_order(order, order.limit_price)

    def _fill_order(self, order: Order, fill_price: float):
        """成交订单，更新持仓和资金"""
        commission = self._calc_commission(order.side, fill_price, order.shares)
        total_cost = fill_price * order.shares + commission

        if order.side == OrderSide.BUY.value:
            if total_cost > self.available_cash:
                order.status = OrderStatus.REJECTED.value
                order.reason = "成交时资金不足"
                return
            self.available_cash -= total_cost
            # 更新持仓
            if order.code in self.positions:
                pos = self.positions[order.code]
                new_shares = pos.shares + order.shares
                pos.avg_cost = (pos.avg_cost * pos.shares + fill_price * order.shares) / new_shares
                pos.shares = new_shares
            else:
                self.positions[order.code] = SimPosition(
                    code=order.code,
                    shares=order.shares,
                    avg_cost=fill_price,
                    current_price=fill_price,
                )
        else:  # SELL
            pos = self.positions.get(order.code)
            if not pos or pos.shares < order.shares:
                order.status = OrderStatus.REJECTED.value
                order.reason = "成交时持仓不足"
                return
            proceeds = fill_price * order.shares - commission
            self.available_cash += proceeds
            pos.shares -= order.shares
            if pos.shares == 0:
                del self.positions[order.code]

        order.filled_price = round(fill_price, 4)
        order.filled_shares = order.shares
        order.commission = commission
        order.status = OrderStatus.FILLED.value
        order.filled_at = datetime.now().isoformat()

        record = {**order.to_dict(), 'total_asset': self._total_asset()}
        self.trade_history.append(record)
        logger.info(
            f"[SimBroker] 成交: {order.side.upper()} {order.code} "
            f"{order.shares}股 @ {fill_price:.2f}，手续费 {commission:.2f}，"
            f"总资产 {self._total_asset():,.2f}"
        )

    # ─── 查询接口 ──────────────────────────────────────────────────────────

    def get_positions(self) -> Dict[str, SimPosition]:
        return dict(self.positions)

    def get_position(self, code: str) -> Optional[SimPosition]:
        return self.positions.get(code)

    def cancel_order(self, order_id: str) -> bool:
        order = self.orders.get(order_id)
        if not order or order.status != OrderStatus.PENDING.value:
            return False
        order.status = OrderStatus.CANCELLED.value
        # 解冻限价买单冻结资金
        if order.side == OrderSide.BUY.value and order.order_type == OrderType.LIMIT.value:
            frozen = order.limit_price * order.shares
            self.available_cash += frozen
            self.frozen_cash -= frozen
        self.save_state()
        return True

    def update_market_price(self, code: str, price: float):
        """更新持仓市价（用于盈亏计算）"""
        if code in self.positions:
            self.positions[code].current_price = price

    def get_account_summary(self) -> Dict[str, Any]:
        total_mv = sum(p.market_value for p in self.positions.values())
        total_unrealized = sum(p.unrealized_pnl for p in self.positions.values())
        total_asset = self.available_cash + self.frozen_cash + total_mv
        return {
            'account_id': self.account_id,
            'initial_capital': self.initial_capital,
            'available_cash': round(self.available_cash, 2),
            'frozen_cash': round(self.frozen_cash, 2),
            'total_market_value': round(total_mv, 2),
            'total_asset': round(total_asset, 2),
            'total_unrealized_pnl': round(total_unrealized, 2),
            'total_pnl': round(total_asset - self.initial_capital, 2),
            'total_pnl_pct': round((total_asset - self.initial_capital) / self.initial_capital * 100, 4),
            'positions_count': len(self.positions),
        }

    def get_trade_history(self, code: str = None, limit: int = 100) -> List[Dict]:
        history = self.trade_history
        if code:
            history = [r for r in history if r.get('code') == code]
        return history[-limit:]

    # ─── 持久化 ───────────────────────────────────────────────────────────

    def save_state(self):
        """保存账户状态到文件"""
        try:
            Path(os.path.dirname(self._state_path)).mkdir(parents=True, exist_ok=True)
            state = {
                'account_id': self.account_id,
                'initial_capital': self.initial_capital,
                'available_cash': self.available_cash,
                'frozen_cash': self.frozen_cash,
                'positions': {k: v.to_dict() for k, v in self.positions.items()},
                'trade_history': self.trade_history[-500:],  # 最多保存最近 500 条
                'order_seq': self._order_seq,
            }
            with open(self._state_path, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存模拟账户状态失败: {e}")

    def load_state(self):
        """从文件加载账户状态"""
        if not os.path.exists(self._state_path):
            return
        try:
            with open(self._state_path, 'r', encoding='utf-8') as f:
                state = json.load(f)
            self.initial_capital = state.get('initial_capital', self.initial_capital)
            self.available_cash = state.get('available_cash', self.initial_capital)
            self.frozen_cash = state.get('frozen_cash', 0.0)
            self.trade_history = state.get('trade_history', [])
            self._order_seq = state.get('order_seq', 0)
            for code, pd_dict in state.get('positions', {}).items():
                self.positions[code] = SimPosition(
                    code=pd_dict['code'],
                    shares=pd_dict['shares'],
                    avg_cost=pd_dict['avg_cost'],
                    current_price=pd_dict.get('current_price', pd_dict['avg_cost']),
                )
            logger.info(f"模拟账户 #{self.account_id} 状态已加载（总资产: ¥{self._total_asset():,.2f}）")
        except Exception as e:
            logger.error(f"加载模拟账户状态失败: {e}")

    def reset(self, initial_capital: float = None):
        """重置账户（清空持仓、恢复初始资金）"""
        if initial_capital:
            self.initial_capital = initial_capital
        self.available_cash = self.initial_capital
        self.frozen_cash = 0.0
        self.positions.clear()
        self.orders.clear()
        self.trade_history.clear()
        self._order_seq = 0
        self.save_state()
        logger.info(f"模拟账户 #{self.account_id} 已重置，初始资金: ¥{self.initial_capital:,.2f}")


# ─── 向后兼容存根 ───────────────────────────────────────────────────────────

class Account:
    """向后兼容的简易账户视图（委托给 SimulatedBroker）"""

    def __init__(self, account_id: int, initial_capital: float = 1_000_000):
        self.account_id = account_id
        self._broker = SimulatedBroker(account_id, initial_capital)

    @property
    def capital(self) -> float:
        return self._broker.available_cash

    @property
    def positions(self) -> Dict:
        return {k: v.to_dict() for k, v in self._broker.get_positions().items()}

    def get_broker(self) -> SimulatedBroker:
        return self._broker

    def __repr__(self):
        return f"Account(id={self.account_id}, capital={self.capital:.2f})"


class AccountManager:
    """账户管理器"""

    def __init__(self):
        self._accounts: Dict[int, Account] = {}
        self.get_account(1)   # 确保默认账户存在

    def get_account(self, account_id: int, initial_capital: float = 1_000_000) -> Account:
        if account_id not in self._accounts:
            self._accounts[account_id] = Account(account_id, initial_capital)
            logger.info(f"模拟账户 #{account_id} 已创建/加载")
        return self._accounts[account_id]

    def list_accounts(self) -> List[Account]:
        return list(self._accounts.values())


account_manager = AccountManager()

