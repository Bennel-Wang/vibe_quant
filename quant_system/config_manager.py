"""
配置管理模块
统一管理所有配置信息

配置文件查找顺序（优先级从高到低）：
  1. 环境变量 QUANT_CONFIG 指定的路径
  2. 项目根目录下的 config.yaml（推荐，已在 .gitignore 中排除）
  3. 旧路径 C:\\Users\\quantization_config.yaml（向后兼容）
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path

# 项目根目录（config_manager.py 的上一级）
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 默认配置文件路径：优先环境变量，其次项目根目录，最后旧路径（向后兼容）
def _resolve_default_config_path() -> str:
    env_path = os.environ.get("QUANT_CONFIG")
    if env_path and os.path.exists(env_path):
        return env_path
    project_path = os.path.join(_PROJECT_ROOT, "config.yaml")
    if os.path.exists(project_path):
        return project_path
    legacy_path = r"C:\Users\quantization_config.yaml"
    if os.path.exists(legacy_path):
        return legacy_path
    # 最终回退到项目根目录（会在加载时报错并给出友好提示）
    return project_path


class ConfigManager:
    """配置管理器"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._config = None
        return cls._instance
    
    def __init__(self, config_path: str = None):
        if self._config is None:
            self.config_path = config_path or _resolve_default_config_path()
            self._load_config()
    
    def _load_config(self):
        """加载配置文件"""
        if not os.path.exists(self.config_path):
            example = os.path.join(_PROJECT_ROOT, "config.yaml.example")
            raise FileNotFoundError(
                f"配置文件不存在: {self.config_path}\n"
                f"请将 {example} 复制为 {os.path.join(_PROJECT_ROOT, 'config.yaml')} "
                f"并填入真实 Token，或通过环境变量 QUANT_CONFIG 指定路径。"
            )
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self._config = yaml.safe_load(f)
        
        # 确保数据目录存在
        self._ensure_directories()
    
    def _ensure_directories(self):
        """确保所有数据目录存在"""
        dirs = [
            self.get('data_storage.data_dir'),
            self.get('data_storage.history_dir'),
            self.get('data_storage.realtime_dir'),
            self.get('data_storage.news_dir'),
            self.get('data_storage.indicators_dir'),
            self.get('data_storage.features_dir'),
            self.get('data_storage.backtest_dir'),
            os.path.dirname(self.get('logging.file')),
        ]
        
        for dir_path in dirs:
            if dir_path:
                Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值，支持点号分隔的键路径
        
        Args:
            key: 配置键，如 'tokens.tushare_token'
            default: 默认值
        
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """
        设置配置值
        
        Args:
            key: 配置键
            value: 配置值
        """
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save(self):
        """保存配置到文件"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self._config, f, allow_unicode=True, default_flow_style=False)
    
    def get_tushare_token(self) -> str:
        """获取Tushare Token"""
        return self.get('tokens.tushare_token', '')
    
    def get_pushplus_token(self) -> str:
        """获取PushPlus Token（向后兼容，取第一个账户的 token）"""
        accounts = self.get('tokens.pushplus_accounts', [])
        if isinstance(accounts, list):
            for acc in accounts:
                if isinstance(acc, dict) and acc.get('enabled') and acc.get('token'):
                    return acc['token']
        return self.get('tokens.pushplus_token', '')
    
    def get_modelscope_token(self) -> str:
        """获取ModelScope Token"""
        return self.get('tokens.modelscope_token', '')
    
    def get_all_tokens(self) -> Dict[str, str]:
        """获取所有Token"""
        return {
            'tushare': self.get_tushare_token(),
            'pushplus': self.get_pushplus_token(),
            'modelscope': self.get_modelscope_token(),
        }
    
    def get_data_dirs(self) -> Dict[str, str]:
        """获取所有数据目录"""
        return {
            'data': self.get('data_storage.data_dir'),
            'history': self.get('data_storage.history_dir'),
            'realtime': self.get('data_storage.realtime_dir'),
            'news': self.get('data_storage.news_dir'),
            'indicators': self.get('data_storage.indicators_dir'),
            'features': self.get('data_storage.features_dir'),
            'backtest': self.get('data_storage.backtest_dir'),
        }
    
    def get_rsi_config(self) -> Dict[str, Any]:
        """获取RSI配置"""
        return self.get('technical_indicators.rsi', {
            'periods': [6, 12, 24],
            'timeframes': ['day', 'week', 'month'],
            'history_lookback': 252
        })
    
    def get_backtest_config(self) -> Dict[str, Any]:
        """获取回测配置"""
        return {
            'initial_capital': self.get('backtest.initial_capital', 1000000),
            'commission_rate': self.get('backtest.commission_rate', 0.0003),
            'slippage': self.get('backtest.slippage', 0.001),
        }
    
    def get_risk_config(self) -> Dict[str, Any]:
        """获取风控配置"""
        return {
            'max_position_ratio': self.get('risk_management.max_position_ratio', 0.8),
            'max_single_stock_ratio': self.get('risk_management.max_single_stock_ratio', 0.3),
            'stop_loss_ratio': self.get('risk_management.stop_loss_ratio', 0.05),
            'take_profit_ratio': self.get('risk_management.take_profit_ratio', 0.1),
            'trailing_stop_ratio': self.get('risk_management.trailing_stop_ratio', 0.0),
            'var_confidence': self.get('risk_management.var_confidence', 0.95),
            'var_window': self.get('risk_management.var_window', 252),
        }

    def get_ai_config(self) -> Dict[str, Any]:
        """获取AI模型配置"""
        return {
            'provider': self.get('ai_models.provider', 'modelscope'),
            'model_name': self.get('ai_models.model_name', 'qwen-max'),
            'max_tokens': self.get('ai_models.max_tokens', 2000),
            'temperature': self.get('ai_models.temperature', 0.7),
            'retry_times': self.get('ai_models.retry_times', 3),
            'retry_delay': self.get('ai_models.retry_delay', 2),
        }

    def get_backtest_config(self) -> Dict[str, Any]:
        """获取回测配置"""
        return {
            'initial_capital': self.get('backtest.initial_capital', 1000000),
            'commission_rate': self.get('backtest.commission_rate', 0.0003),
            'slippage': self.get('backtest.slippage', 0.001),
            'benchmark_code': self.get('backtest.benchmark_code', '000300.SH'),
        }

    def get_scheduler_config(self) -> Dict[str, Any]:
        """获取调度器配置"""
        return {
            'enabled': self.get('scheduler.enabled', True),
            'afternoon_times': self.get('scheduler.afternoon_times', ['14:25', '14:45']),
            'alert_on_failure': self.get('scheduler.alert_on_failure', True),
            'task_timeout': self.get('scheduler.task_timeout', 300),
        }

    def get_web_config(self) -> Dict[str, Any]:
        """获取Web服务配置"""
        return {
            'host': self.get('web.host', '0.0.0.0'),
            'port': self.get('web.port', 8080),
            'debug': self.get('web.debug', False),
            'auth_enabled': self.get('web.auth_enabled', False),
            'username': self.get('tokens.web_username', 'admin'),
            'password': self.get('tokens.web_password', ''),
        }


# 全局配置实例
config = ConfigManager()
