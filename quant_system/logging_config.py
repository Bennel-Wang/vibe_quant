"""
集中化日志配置模块
统一处理日志格式、轮转、级别
"""

import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logging(config=None):
    """
    配置全局日志

    Args:
        config: ConfigManager 实例（可选）。传入时从配置文件读取日志参数；
                不传时使用合理默认值，确保在配置加载前也能记录日志。
    """
    if config is not None:
        log_level_str = config.get('logging.level', 'INFO')
        log_file = config.get('logging.file', './logs/quant_system.log')
        max_bytes = config.get('logging.max_bytes', 10 * 1024 * 1024)
        backup_count = config.get('logging.backup_count', 5)
    else:
        log_level_str = os.environ.get('LOG_LEVEL', 'INFO')
        log_file = './logs/quant_system.log'
        max_bytes = 10 * 1024 * 1024
        backup_count = 5

    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    root_logger = logging.getLogger()
    # 避免重复添加 handler
    if root_logger.handlers:
        return

    root_logger.setLevel(log_level)

    # 滚动文件 handler
    fh = RotatingFileHandler(log_file, maxBytes=max_bytes,
                             backupCount=backup_count, encoding='utf-8')
    fh.setFormatter(formatter)
    root_logger.addHandler(fh)

    # 控制台 handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)
