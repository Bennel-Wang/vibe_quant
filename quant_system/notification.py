"""
消息通知模块
使用PushPlus进行微信消息推送，同时支持SMTP邮件发送
"""

import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests
from typing import List, Optional, Dict
from datetime import datetime

from .config_manager import config

logger = logging.getLogger(__name__)


class PushPlusNotifier:
    """PushPlus消息推送器"""
    
    API_URL = "http://www.pushplus.plus/send"
    
    def __init__(self, token: str = None):
        self.token = token or config.get_pushplus_token()
        if not self.token:
            logger.warning("PushPlus Token未配置")
    
    def send_message(self, title: str, content: str, 
                     template: str = "txt") -> bool:
        """
        发送文本消息
        
        Args:
            title: 消息标题
            content: 消息内容
            template: 模板类型 (txt/html/json/markdown)
        
        Returns:
            是否发送成功
        """
        if not self.token:
            logger.error("PushPlus Token未配置，无法发送消息")
            return False
        
        data = {
            "token": self.token,
            "title": title,
            "content": content,
            "template": template,
        }
        
        try:
            response = requests.post(
                self.API_URL,
                data=data,
                timeout=10
            )
            result = response.json()
            
            if result.get("code") == 200:
                logger.info(f"消息发送成功: {title}")
                return True
            else:
                logger.error(f"消息发送失败: {result.get('msg')}")
                return False
                
        except Exception as e:
            logger.error(f"消息发送异常: {e}")
            return False
    
    def send_html_message(self, title: str, content: str) -> bool:
        """发送HTML格式消息"""
        return self.send_message(title, content, template="html")
    
    def send_markdown_message(self, title: str, content: str) -> bool:
        """发送Markdown格式消息"""
        return self.send_message(title, content, template="markdown")
    
    def send_json_message(self, title: str, data: Dict) -> bool:
        """发送JSON格式消息"""
        content = json.dumps(data, ensure_ascii=False, indent=2)
        return self.send_message(title, content, template="json")


class EmailNotifier:
    """SMTP邮件通知器"""
    
    def __init__(self, smtp_host: str = None, smtp_port: int = None,
                 username: str = None, password: str = None,
                 sender: str = None, receiver: str = None):
        self.smtp_host = smtp_host or config.get('notification.email.smtp_host', '')
        self.smtp_port = smtp_port or config.get('notification.email.smtp_port', 465)
        self.username = username or config.get('notification.email.username', '')
        self.password = password or config.get('notification.email.password', '')
        self.sender = sender or config.get('notification.email.sender', self.username)
        self.receiver = receiver or config.get('notification.email.receiver', '')
    
    @property
    def is_configured(self) -> bool:
        return bool(self.smtp_host and self.username and self.password and self.receiver)
    
    def send_email(self, subject: str, body: str, html: bool = False) -> bool:
        """发送邮件"""
        if not self.is_configured:
            logger.warning("邮件配置不完整，无法发送")
            return False
        
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.sender
            msg['To'] = self.receiver
            msg['Subject'] = subject
            
            content_type = 'html' if html else 'plain'
            msg.attach(MIMEText(body, content_type, 'utf-8'))
            
            port = int(self.smtp_port)
            if port == 465:
                server = smtplib.SMTP_SSL(self.smtp_host, port, timeout=10)
            else:
                server = smtplib.SMTP(self.smtp_host, port, timeout=10)
                server.starttls()
            
            server.login(self.username, self.password)
            server.sendmail(self.sender, self.receiver.split(','), msg.as_string())
            server.quit()
            logger.info(f"邮件发送成功: {subject}")
            return True
        except Exception as e:
            logger.error(f"邮件发送失败: {e}")
            return False


class NotificationManager:
    """通知管理器 - 支持微信(PushPlus)和邮件(SMTP)"""

    # 同类通知 4 小时内只发一次，防当天多次重复推送
    _DEDUP_WINDOW = 4 * 3600

    def __init__(self):
        self.notifier = PushPlusNotifier()
        self.email_notifier = EmailNotifier()
        self.wechat_enabled = bool(config.get_pushplus_token())
        self.email_enabled = self.email_notifier.is_configured
        self.enabled = self.wechat_enabled or self.email_enabled
        self._last_sent: Dict[str, float] = {}  # dedup: title前缀 -> 上次发送时间戳
        self._dedup_lock = __import__('threading').Lock()

    def _is_duplicate(self, title: str) -> bool:
        """同标题前缀 4 小时内只发一次，返回 True 表示应跳过（线程安全）"""
        import time
        key = title[:20]
        now = time.time()
        with self._dedup_lock:
            if now - self._last_sent.get(key, 0) < self._DEDUP_WINDOW:
                return True
            self._last_sent[key] = now
            return False

    def _send(self, title: str, content: str, channels: List[str] = None):
        """统一发送到所有启用的渠道"""
        channels = channels or ['wechat', 'email']
        results = {}
        if 'wechat' in channels and self.wechat_enabled:
            results['wechat'] = self.notifier.send_markdown_message(title, content)
        if 'email' in channels and self.email_enabled:
            results['email'] = self.email_notifier.send_email(title, content)
        return results

    def send_markdown_message(self, title: str, content: str, channels: List[str] = None):
        """发送 Markdown 消息，同标题 2 小时内自动去重"""
        if self._is_duplicate(title):
            logger.info(f"通知去重，跳过重复发送: {title[:40]}")
            return {'dedup': True}
        return self._send(title, content, channels)

    def get_config_status(self) -> Dict:
        """获取通知配置状态"""
        return {
            'wechat': {
                'enabled': self.wechat_enabled,
                'token_configured': bool(config.get_pushplus_token()),
            },
            'email': {
                'enabled': self.email_enabled,
                'smtp_host': self.email_notifier.smtp_host or '',
                'receiver': self.email_notifier.receiver or '',
            }
        }
    
    def send_trade_notification(self, code: str, action: str, 
                                 shares: int, price: float, 
                                 reason: str = ""):
        """
        发送交易通知
        
        Args:
            code: 股票代码
            action: 操作类型 (buy/sell)
            shares: 股数
            price: 价格
            reason: 操作理由
        """
        if not self.enabled:
            return
        
        from .stock_manager import stock_manager
        stock = stock_manager.get_stock_by_code(code)
        name = stock.name if stock else code
        
        action_text = "买入" if action == "buy" else "卖出"
        amount = shares * price
        
        title = f"交易提醒: {name} {action_text}"
        content = f"""
## 交易详情

- **股票**: {name} ({code})
- **操作**: {action_text}
- **数量**: {shares} 股
- **价格**: ¥{price:.2f}
- **金额**: ¥{amount:,.2f}
- **时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### 操作理由
{reason}
"""
        
        self._send(title, content)
    
    def send_strategy_signal(self, code: str, strategy_name: str,
                             action: str, confidence: float, 
                             reasoning: str):
        """发送策略信号通知"""
        if not self.enabled:
            return
        
        from .stock_manager import stock_manager
        stock = stock_manager.get_stock_by_code(code)
        name = stock.name if stock else code
        
        action_emoji = {
            "buy": "📈",
            "sell": "📉",
            "hold": "➡️"
        }.get(action, "❓")
        
        title = f"策略信号: {name} - {action.upper()}"
        content = f"""
## 策略信号

- **股票**: {name} ({code})
- **策略**: {strategy_name}
- **信号**: {action_emoji} {action.upper()}
- **置信度**: {confidence:.1%}
- **时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### 决策理由
{reasoning}
"""
        
        self._send(title, content)
    
    def send_risk_alert(self, alerts: List[Dict]):
        """
        发送风险预警
        
        Args:
            alerts: 预警列表
        """
        if not self.enabled or not alerts:
            return
        
        title = f"⚠️ 风险预警: {len(alerts)} 只股票需要关注"
        
        content = "## 风险预警\n\n"
        for alert in alerts:
            content += f"- **{alert['name']}({alert['code']})**: {alert['reason']}\n"
        
        content += f"\n**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        self._send(title, content)
    
    def send_daily_report(self, report_data: Dict):
        """
        发送每日报告
        
        Args:
            report_data: 报告数据
        """
        if not self.enabled:
            return
        
        title = f"📊 每日报告 ({datetime.now().strftime('%Y-%m-%d')})"
        
        content = f"""
## 每日投资报告

### 账户概况
- 总资产: ¥{report_data.get('total_capital', 0):,.2f}
- 可用资金: ¥{report_data.get('available_cash', 0):,.2f}
- 当日盈亏: ¥{report_data.get('daily_pnl', 0):,.2f} ({report_data.get('daily_pnl_pct', 0):+.2f}%)

### 持仓概况
- 持仓数量: {report_data.get('positions_count', 0)} 只
- 总仓位: {report_data.get('position_ratio', 0):.1%}

### 今日信号
"""
        
        signals = report_data.get('signals', [])
        if signals:
            for signal in signals:
                content += f"- {signal}\n"
        else:
            content += "- 今日无交易信号\n"
        
        content += f"\n**报告时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        self._send(title, content)
    
    def send_backtest_report(self, code: str, strategy_name: str, 
                             result_summary: Dict):
        """
        发送回测报告
        
        Args:
            code: 股票代码
            strategy_name: 策略名称
            result_summary: 回测结果摘要
        """
        if not self.enabled:
            return
        
        from .stock_manager import stock_manager
        stock = stock_manager.get_stock_by_code(code)
        name = stock.name if stock else code
        
        title = f"📈 回测报告: {name} - {strategy_name}"
        
        content = f"""
## 回测结果

### 基本信息
- **股票**: {name} ({code})
- **策略**: {strategy_name}
- **回测区间**: {result_summary.get('start_date', '')} ~ {result_summary.get('end_date', '')}

### 收益指标
- 总收益: {result_summary.get('total_return_pct', 0):+.2f}%
- 年化收益: {result_summary.get('annual_return', 0):.2f}%
- 夏普比率: {result_summary.get('sharpe_ratio', 0):.2f}

### 风险指标
- 最大回撤: {result_summary.get('max_drawdown_pct', 0):.2f}%

### 交易统计
- 总交易次数: {result_summary.get('total_trades', 0)}
- 胜率: {result_summary.get('win_rate', 0):.2f}%
- 盈亏比: {result_summary.get('profit_factor', 0):.2f}

**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        self._send(title, content)
    
    def send_system_notification(self, message: str, level: str = "info"):
        """
        发送系统通知
        
        Args:
            message: 消息内容
            level: 级别 (info/warning/error)
        """
        if not self.enabled:
            return
        
        level_emoji = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌"
        }.get(level, "ℹ️")
        
        title = f"{level_emoji} 系统通知"
        content = f"{message}\n\n**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        self._send(title, content)


# 全局实例
notification_manager = NotificationManager()
