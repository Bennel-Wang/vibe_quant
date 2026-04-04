"""
风控模块
仓位管理、止损止盈、风险约束
"""

import os
import json
import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

from .config_manager import config
from .stock_manager import stock_manager

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """持仓信息"""
    code: str
    name: str
    shares: int
    avg_cost: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float
    prev_close: float = 0.0
    today_pnl: float = 0.0
    today_pnl_pct: float = 0.0


@dataclass
class RiskCheckResult:
    """风控检查结果"""
    passed: bool
    code: str
    action: str
    message: str
    suggested_position: float
    risk_level: str  # low/medium/high


class RiskManager:
    """风险管理器"""
    
    def __init__(self):
        self.risk_config = config.get_risk_config()
        self.max_position_ratio = self.risk_config['max_position_ratio']
        self.max_single_stock_ratio = self.risk_config['max_single_stock_ratio']
        self.stop_loss_ratio = self.risk_config['stop_loss_ratio']
        self.take_profit_ratio = self.risk_config['take_profit_ratio']
        
        # 当前状态
        self.total_capital: float = 1000000
        self.available_cash: float = 1000000
        self.positions: Dict[str, Position] = {}
        self.position_history: List[Dict] = []

        # 累计盈亏基准
        # initial_capital_baseline == 0 表示未设置（首次使用时自动以当前 total_capital 为基准）
        self.initial_capital_baseline: float = 0.0
        # 已实现盈亏（记录清仓/减仓时锁定的利润）
        self.realized_pnl: float = 0.0
    
    def update_capital(self, total_capital: float, available_cash: float):
        """更新资金信息"""
        self.total_capital = total_capital
        self.available_cash = available_cash

    def set_initial_capital_baseline(self, baseline: float):
        """
        设置累计盈亏基准（起始本金）

        Args:
            baseline: 基准金额；传 0 则以当前总资产（可用资金+持仓市值）自动设定
        """
        if baseline > 0:
            self.initial_capital_baseline = baseline
        else:
            total_position_value = sum(p.market_value for p in self.positions.values())
            self.initial_capital_baseline = self.available_cash + total_position_value
        logger.info(f"累计盈亏基准已设置为: ¥{self.initial_capital_baseline:,.2f}")

    def get_cumulative_pnl(self) -> Dict[str, float]:
        """
        计算累计盈亏

        Returns:
            {
              'initial_capital_baseline': 起始基准,
              'cumulative_pnl': 当前净值 - 基准,
              'cumulative_pnl_pct': 累计收益率(%),
              'unrealized_pnl': 当前浮动盈亏,
            }
        """
        unrealized = sum(p.unrealized_pnl for p in self.positions.values())
        total_position_value = sum(p.market_value for p in self.positions.values())
        # 当前净值 = 可用资金 + 持仓市值（随行情浮动）
        current_nav = self.available_cash + total_position_value
        baseline = self.initial_capital_baseline if self.initial_capital_baseline > 0 else current_nav
        cumulative_pnl = current_nav - baseline
        cumulative_pnl_pct = (cumulative_pnl / baseline * 100) if baseline > 0 else 0.0
        return {
            'initial_capital_baseline': baseline,
            'cumulative_pnl': round(cumulative_pnl, 2),
            'cumulative_pnl_pct': round(cumulative_pnl_pct, 4),
            'unrealized_pnl': round(unrealized, 2),
        }
    
    def update_position(self, code: str, shares: int, avg_cost: float, 
                        current_price: float, prev_close: float = 0.0):
        """更新持仓信息"""
        stock = stock_manager.get_stock_by_code(code)
        name = stock.name if stock else code
        
        market_value = shares * current_price
        unrealized_pnl = shares * (current_price - avg_cost)
        unrealized_pnl_pct = (current_price - avg_cost) / avg_cost * 100 if avg_cost > 0 else 0
        
        # 今日盈亏：基于昨日收盘价
        if prev_close > 0:
            today_pnl = shares * (current_price - prev_close)
            today_pnl_pct = (current_price - prev_close) / prev_close * 100
        else:
            today_pnl = 0.0
            today_pnl_pct = 0.0
        
        self.positions[code] = Position(
            code=code,
            name=name,
            shares=shares,
            avg_cost=avg_cost,
            current_price=current_price,
            market_value=market_value,
            unrealized_pnl=unrealized_pnl,
            unrealized_pnl_pct=unrealized_pnl_pct,
            prev_close=prev_close,
            today_pnl=today_pnl,
            today_pnl_pct=today_pnl_pct
        )
    
    def check_position_limit(self, code: str, proposed_shares: int, 
                             price: float) -> RiskCheckResult:
        """
        检查仓位限制
        
        Args:
            code: 股票代码
            proposed_shares: 拟买入股数
            price: 当前价格
        
        Returns:
            风控检查结果
        """
        proposed_value = proposed_shares * price
        current_value = self.positions.get(code, Position(code, "", 0, 0, 0, 0, 0)).market_value
        total_value = proposed_value + current_value
        
        # 动态总资产 = 可用资金 + 持仓市值
        dyn_total = self.available_cash + sum(p.market_value for p in self.positions.values())
        if dyn_total <= 0:
            dyn_total = 1  # avoid division by zero

        # 检查单只股票仓位限制
        single_stock_ratio = total_value / dyn_total
        if single_stock_ratio > self.max_single_stock_ratio:
            max_value = dyn_total * self.max_single_stock_ratio
            max_additional = max(0, max_value - current_value)
            suggested_shares = int(max_additional / price / 100) * 100
            
            return RiskCheckResult(
                passed=False,
                code=code,
                action="reject",
                message=f"单只股票仓位将超过限制 ({single_stock_ratio:.1%} > {self.max_single_stock_ratio:.1%})",
                suggested_position=suggested_shares,
                risk_level="high"
            )
        
        # 检查总仓位限制
        total_position_value = sum(p.market_value for p in self.positions.values()) + proposed_value
        total_position_ratio = total_position_value / dyn_total
        
        if total_position_ratio > self.max_position_ratio:
            return RiskCheckResult(
                passed=False,
                code=code,
                action="reject",
                message=f"总仓位将超过限制 ({total_position_ratio:.1%} > {self.max_position_ratio:.1%})",
                suggested_position=0,
                risk_level="high"
            )
        
        return RiskCheckResult(
            passed=True,
            code=code,
            action="allow",
            message="仓位检查通过",
            suggested_position=proposed_shares,
            risk_level="low"
        )
    
    def check_stop_loss_take_profit(self, code: str) -> Optional[RiskCheckResult]:
        """
        检查止损止盈
        
        Args:
            code: 股票代码
        
        Returns:
            风控检查结果，如果不需要操作则返回None
        """
        position = self.positions.get(code)
        if not position or position.shares == 0:
            return None
        
        pnl_pct = position.unrealized_pnl_pct
        
        # 检查止损
        if pnl_pct <= -self.stop_loss_ratio * 100:
            return RiskCheckResult(
                passed=False,
                code=code,
                action="sell",
                message=f"触发止损 ({pnl_pct:.2f}% <= -{self.stop_loss_ratio*100:.1f}%)",
                suggested_position=position.shares,
                risk_level="high"
            )
        
        # 检查止盈
        if pnl_pct >= self.take_profit_ratio * 100:
            return RiskCheckResult(
                passed=False,
                code=code,
                action="sell",
                message=f"触发止盈 ({pnl_pct:.2f}% >= {self.take_profit_ratio*100:.1f}%)",
                suggested_position=position.shares,
                risk_level="medium"
            )
        
        return None
    
    def check_risk_before_trade(self, code: str, action: str, 
                                 shares: int, price: float) -> RiskCheckResult:
        """
        交易前风险检查
        
        Args:
            code: 股票代码
            action: 操作类型 (buy/sell)
            shares: 股数
            price: 价格
        
        Returns:
            风控检查结果
        """
        if action == "buy":
            # 检查仓位限制
            result = self.check_position_limit(code, shares, price)
            if not result.passed:
                return result
            
            # 检查资金
            required_cash = shares * price * 1.003  # 包含手续费
            if required_cash > self.available_cash:
                max_shares = int(self.available_cash / price / 1.003 / 100) * 100
                return RiskCheckResult(
                    passed=False,
                    code=code,
                    action="adjust",
                    message="资金不足",
                    suggested_position=max_shares,
                    risk_level="medium"
                )
        
        elif action == "sell":
            # 检查持仓
            position = self.positions.get(code)
            if not position or position.shares < shares:
                available = position.shares if position else 0
                return RiskCheckResult(
                    passed=False,
                    code=code,
                    action="adjust",
                    message=f"持仓不足 (可用: {available})",
                    suggested_position=available,
                    risk_level="medium"
                )
        
        return RiskCheckResult(
            passed=True,
            code=code,
            action="allow",
            message="风险检查通过",
            suggested_position=shares,
            risk_level="low"
        )
    
    def get_portfolio_risk(self) -> Dict[str, Any]:
        """
        获取组合风险指标
        
        Returns:
            风险指标字典
        """
        total_position_value = sum(p.market_value for p in self.positions.values())
        # 总资产动态计算 = 可用资金 + 持仓市值（随行情变化）
        total_capital = self.available_cash + total_position_value
        total_position_ratio = total_position_value / total_capital if total_capital > 0 else 0
        
        # 计算集中度
        if self.positions:
            max_position = max(p.market_value for p in self.positions.values())
            concentration = max_position / total_position_value if total_position_value > 0 else 0
        else:
            concentration = 0
        
        # 计算盈亏
        total_unrealized_pnl = sum(p.unrealized_pnl for p in self.positions.values())
        total_today_pnl = sum(p.today_pnl for p in self.positions.values())
        
        # 需要止损的股票
        stop_loss_alerts = []
        for code, position in self.positions.items():
            result = self.check_stop_loss_take_profit(code)
            if result and result.action == "sell":
                stop_loss_alerts.append({
                    'code': code,
                    'name': position.name,
                    'pnl_pct': position.unrealized_pnl_pct,
                    'reason': result.message
                })
        
        return {
            'total_capital': total_capital,
            'available_cash': self.available_cash,
            'total_position_value': total_position_value,
            'position_ratio': total_position_ratio,
            'concentration': concentration,
            'total_unrealized_pnl': total_unrealized_pnl,
            'total_today_pnl': total_today_pnl,
            'positions_count': len(self.positions),
            'stop_loss_alerts': stop_loss_alerts,
            'risk_level': self._assess_risk_level(total_position_ratio, concentration),
            **self.get_cumulative_pnl(),
        }
    
    def _assess_risk_level(self, position_ratio: float, concentration: float) -> str:
        """评估风险等级"""
        if position_ratio > self.max_position_ratio * 0.9 or concentration > 0.5:
            return "high"
        elif position_ratio > self.max_position_ratio * 0.7 or concentration > 0.3:
            return "medium"
        else:
            return "low"
    
    def get_position_summary(self) -> pd.DataFrame:
        """
        获取持仓汇总
        
        Returns:
            DataFrame
        """
        if not self.positions:
            return pd.DataFrame()
        
        data = []
        for position in self.positions.values():
            data.append({
                'code': position.code,
                'name': position.name,
                'shares': position.shares,
                'avg_cost': position.avg_cost,
                'current_price': position.current_price,
                'market_value': position.market_value,
                'unrealized_pnl': position.unrealized_pnl,
                'unrealized_pnl_pct': position.unrealized_pnl_pct,
                'prev_close': position.prev_close,
                'today_pnl': position.today_pnl,
                'today_pnl_pct': position.today_pnl_pct,
                'position_ratio': position.market_value / self.total_capital,
            })
        
        return pd.DataFrame(data)
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'total_capital': self.total_capital,
            'available_cash': self.available_cash,
            'initial_capital_baseline': self.initial_capital_baseline,
            'realized_pnl': self.realized_pnl,
            'positions': {
                code: {
                    'code': p.code,
                    'name': p.name,
                    'shares': p.shares,
                    'avg_cost': p.avg_cost,
                    'current_price': p.current_price,
                }
                for code, p in self.positions.items()
            }
        }
    
    def from_dict(self, data: Dict):
        """从字典加载"""
        self.total_capital = data.get('total_capital', 1000000)
        self.available_cash = data.get('available_cash', 1000000)
        self.initial_capital_baseline = data.get('initial_capital_baseline', 0.0)
        self.realized_pnl = data.get('realized_pnl', 0.0)
        
        for code, p_data in data.get('positions', {}).items():
            self.update_position(
                code=p_data['code'],
                shares=p_data['shares'],
                avg_cost=p_data['avg_cost'],
                current_price=p_data['current_price']
            )
    
    def assess_portfolio(self) -> Dict:
        """Alias for get_portfolio_risk - used by API"""
        return self.get_portfolio_risk()
    
    def save_state(self, path: str = None):
        """保存风控状态到文件"""
        if path is None:
            data_dir = Path(config.get('data_storage.data_dir', './data'))
            data_dir.mkdir(parents=True, exist_ok=True)
            path = str(data_dir / 'risk_manager_state.json')
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
            logger.info(f"风控状态已保存: {path}")
        except Exception as e:
            logger.error(f"保存风控状态失败: {e}")
    
    def load_state(self, path: str = None):
        """从文件加载风控状态"""
        if path is None:
            data_dir = Path(config.get('data_storage.data_dir', './data'))
            path = str(data_dir / 'risk_manager_state.json')
        if not os.path.exists(path):
            logger.info(f"风控状态文件不存在，使用默认状态: {path}")
            return
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.from_dict(data)
            logger.info(f"风控状态已加载: {path}")
        except Exception as e:
            logger.error(f"加载风控状态失败: {e}")


class RiskReportGenerator:
    """风险报告生成器"""
    
    def generate_report(self, risk_manager: RiskManager) -> str:
        """
        生成风险报告
        
        Args:
            risk_manager: 风险管理器
        
        Returns:
            报告文本
        """
        portfolio_risk = risk_manager.get_portfolio_risk()
        position_df = risk_manager.get_position_summary()
        
        report = f"""
=== 风险报告 ===
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

【资金概况】
- 总资金: ¥{portfolio_risk['total_capital']:,.2f}
- 可用资金: ¥{portfolio_risk['available_cash']:,.2f}
- 持仓市值: ¥{portfolio_risk['total_position_value']:,.2f}
- 仓位比例: {portfolio_risk['position_ratio']:.1%}
- 浮动盈亏: ¥{portfolio_risk['total_unrealized_pnl']:,.2f}

【风险指标】
- 风险等级: {portfolio_risk['risk_level'].upper()}
- 持仓集中度: {portfolio_risk['concentration']:.1%}
- 持仓股票数: {portfolio_risk['positions_count']}

【风控提醒】
"""
        
        if portfolio_risk['stop_loss_alerts']:
            report += "⚠️ 需要关注:\n"
            for alert in portfolio_risk['stop_loss_alerts']:
                report += f"  - {alert['name']}({alert['code']}): {alert['reason']}\n"
        else:
            report += "✅ 暂无风控提醒\n"
        
        if not position_df.empty:
            report += "\n【持仓明细】\n"
            for _, row in position_df.iterrows():
                report += f"- {row['name']}({row['code']}): {row['shares']}股, 盈亏: {row['unrealized_pnl_pct']:+.2f}%\n"
        
        return report


# 全局实例
risk_manager = RiskManager()
risk_report_generator = RiskReportGenerator()
