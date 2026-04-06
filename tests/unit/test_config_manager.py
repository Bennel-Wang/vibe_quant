"""
配置管理器单元测试

注意: ConfigManager 是单例。测试通过在 __init__ 之前清除 _instance._config
来强制重新加载，或直接访问 _config 字典。
"""
import pytest
import sys
import os
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


@pytest.fixture
def full_config_data(tmp_path):
    """返回 (config_dict, config_file_path) 元组"""
    data = {
        'data_storage': {
            'base_dir': str(tmp_path / 'data'),
            'data_dir': str(tmp_path / 'data'),
            'history_dir': str(tmp_path / 'data' / 'history'),
            'realtime_dir': str(tmp_path / 'data' / 'realtime'),
            'news_dir': str(tmp_path / 'data' / 'news'),
            'indicators_dir': str(tmp_path / 'data' / 'indicators'),
            'features_dir': str(tmp_path / 'data' / 'features'),
            'backtest_dir': str(tmp_path / 'data' / 'backtests'),
        },
        'logging': {'file': str(tmp_path / 'logs' / 'app.log'), 'level': 'DEBUG'},
        'notification': {'dingtalk_webhook': '', 'enabled': False},
        'ai_models': {
            'provider': 'modelscope',
            'model_name': 'Qwen/Test',
            'max_tokens': 2000,
            'temperature': 0.7,
            'retry_times': 3,
            'retry_delay': 1,
        },
        'web': {
            'host': '127.0.0.1',
            'port': 5001,
            'debug': False,
            'auth_enabled': True,
            'username': 'admin',
            'password': 'secret',
        },
        'risk_management': {
            'max_position_ratio': 0.80,
            'max_single_stock_ratio': 0.25,
            'stop_loss_ratio': 0.07,
            'take_profit_ratio': 0.12,
            'trailing_stop_ratio': 0.04,
            'var_confidence': 0.95,
            'var_window': 60,
        },
        'backtest': {
            'commission_rate': 0.0003,
            'benchmark_code': '000300.SH',
        },
        'scheduler': {
            'interval_minutes': 30,
            'alert_on_failure': True,
        },
    }
    # 创建目录
    for d in ['data', 'logs']:
        (tmp_path / d).mkdir(parents=True, exist_ok=True)
    cfg_file = tmp_path / 'config.yaml'
    cfg_file.write_text(yaml.dump(data, allow_unicode=True))
    return data, str(cfg_file)


@pytest.fixture
def cm(full_config_data):
    """创建一个加载了临时配置的 ConfigManager（重置单例状态）"""
    _, cfg_path = full_config_data
    from quant_system.config_manager import ConfigManager
    # 重置单例以便加载新配置
    inst = ConfigManager._instance
    if inst is not None:
        inst._config = None
    manager = ConfigManager.__new__(ConfigManager)
    manager._config = None
    manager.config_path = cfg_path
    manager._load_config()
    return manager


class TestBasicConfig:
    def test_web_config(self, cm):
        web = cm.get_web_config()
        assert web['host'] == '127.0.0.1'
        assert web['port'] == 5001
        assert web['auth_enabled'] is True

    def test_ai_config(self, cm):
        ai = cm.get_ai_config()
        assert ai['provider'] == 'modelscope'
        assert ai['retry_times'] == 3

    def test_risk_config(self, cm):
        risk = cm.get_risk_config()
        assert risk['trailing_stop_ratio'] == 0.04
        assert risk['var_confidence'] == 0.95

    def test_backtest_config(self, cm):
        bt = cm.get_backtest_config()
        assert bt['benchmark_code'] == '000300.SH'

    def test_scheduler_config(self, cm):
        sched = cm.get_scheduler_config()
        assert sched['alert_on_failure'] is True


class TestDefaultFallback:
    def test_missing_trailing_stop_defaults(self, tmp_path):
        """risk.trailing_stop_ratio 不在配置中时应有合理默认值（float 型）"""
        from quant_system.config_manager import ConfigManager
        (tmp_path / 'logs').mkdir(exist_ok=True)
        (tmp_path / 'data').mkdir(exist_ok=True)
        minimal = {
            'data_storage': {
                'base_dir': str(tmp_path / 'data'),
                'data_dir': str(tmp_path / 'data'),
            },
            'logging': {'file': str(tmp_path / 'logs' / 'app.log')},
            'notification': {'enabled': False},
        }
        cfg_file = tmp_path / 'minimal.yaml'
        cfg_file.write_text(yaml.dump(minimal))
        mgr = ConfigManager.__new__(ConfigManager)
        mgr._config = None
        mgr.config_path = str(cfg_file)
        mgr._load_config()
        risk = mgr.get_risk_config()
        assert isinstance(risk.get('trailing_stop_ratio', 0.0), float)


class TestEnvOverride:
    def test_env_var_path(self, full_config_data, monkeypatch):
        """QUANT_CONFIG 环境变量应优先于默认路径"""
        _, cfg_path = full_config_data
        monkeypatch.setenv('QUANT_CONFIG', cfg_path)
        from quant_system.config_manager import _resolve_default_config_path
        resolved = _resolve_default_config_path()
        assert resolved == cfg_path
