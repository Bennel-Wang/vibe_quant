"""
特征/因子提取模块
利用AI分析提取股票特征，判断最适合的投资策略类型
"""

import os
import json
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

from .config_manager import config
from .stock_manager import stock_manager
from .indicators import indicator_analyzer
from .news_collector import sentiment_analyzer

logger = logging.getLogger(__name__)


class AIModelClient:
    """AI模型客户端"""
    
    def __init__(self):
        self.ai_config = config.get_ai_config()
        self.provider = self.ai_config.get('provider', 'modelscope')
        self.token = config.get_modelscope_token()
    
    def call(self, prompt: str, system_prompt: str = None) -> str:
        """
        调用AI模型
        
        Args:
            prompt: 用户提示
            system_prompt: 系统提示
        
        Returns:
            AI响应文本
        """
        if self.provider == 'modelscope':
            return self._call_modelscope(prompt, system_prompt)
        else:
            return self._call_mock(prompt, system_prompt)
    
    def _call_modelscope(self, prompt: str, system_prompt: str = None) -> str:
        """调用ModelScope API"""
        try:
            import http.client
            import socket
            
            conn = http.client.HTTPSConnection("api.modelscope.cn", timeout=15)
            
            payload = json.dumps({
                "input": {
                    "prompt": prompt,
                    "system": system_prompt or "你是一个专业的量化投资分析师。"
                },
                "parameters": {
                    "max_tokens": self.ai_config.get('max_tokens', 2000),
                    "temperature": self.ai_config.get('temperature', 0.7),
                }
            })
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            conn.request("POST", "/api/v1/studio/iic/nlp_qwen_chat/gradio/api/predict",
                        payload, headers)
            
            res = conn.getresponse()
            data = res.read()
            result = json.loads(data.decode("utf-8"))
            
            if 'data' in result:
                return result['data']
            else:
                return result.get('text', '')
                
        except Exception as e:
            # 处理网络类错误（例如Windows上的 getaddrinfo 11001）并自动退回到本地模式
            import socket as _socket
            err_no = getattr(e, 'errno', None)
            if isinstance(e, OSError) and err_no in (11001,):
                logger.warning(f"ModelScope 网络错误 (getaddrinfo 失败, errno={err_no})，已切换为本地AI回退: {e}")
                # 将 provider 切换为本地，避免短时间内重复尝试网络调用
                try:
                    self.provider = 'local'
                except Exception:
                    pass
                return self._call_mock(prompt, system_prompt)
            else:
                logger.error(f"ModelScope API调用失败: {e}")
                return self._call_mock(prompt, system_prompt)
    
    def _call_mock(self, prompt: str, system_prompt: str = None) -> str:
        """模拟AI响应（当API不可用时）"""
        # 这里返回一个基于规则的简单分析
        return """{
            "strategy_type": "trend_following",
            "confidence": 0.7,
            "reasoning": "基于技术指标分析，该股票呈现明显的趋势特征。",
            "recommended_indicators": ["ma", "macd", "rsi"],
            "risk_level": "medium"
        }"""


class FeatureExtractor:
    """特征提取器"""
    
    def __init__(self):
        self.data_dir = config.get('data_storage.features_dir', './data/features')
        self.ai_client = AIModelClient()
        self._ensure_dir()
    
    def _ensure_dir(self):
        """确保数据目录存在"""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
    
    def _get_feature_path(self, code: str) -> str:
        """获取特征存储路径"""
        return os.path.join(self.data_dir, f"{code}_features.json")
    
    def extract_technical_features(self, code: str) -> Dict[str, Any]:
        """
        提取技术特征
        
        Args:
            code: 股票代码
        
        Returns:
            技术特征字典
        """
        signals = indicator_analyzer.get_latest_signals(code)
        
        if not signals:
            return {}
        
        features = {
            'trend_strength': abs(signals.get('overall_score', 0)) / 100,
            'trend_direction': 1 if signals.get('overall_score', 0) > 0 else -1,
            'rsi_level': signals.get('rsi_6', 50) / 100,
            'macd_momentum': 1 if signals.get('macd_histogram', 0) > 0 else -1,
            'ma_alignment': 1 if '多头' in signals.get('ma_trend', '') else -1,
            'volatility_proxy': abs(signals.get('kdj_j', 50) - 50) / 50,
            'bollinger_position': signals.get('boll_position', 0.5),
        }
        
        return features
    
    def extract_sentiment_features(self, code: str) -> Dict[str, Any]:
        """
        提取情感特征
        
        Args:
            code: 股票代码
        
        Returns:
            情感特征字典
        """
        daily_sentiment = sentiment_analyzer.get_daily_sentiment(code)
        
        if daily_sentiment.empty:
            return {
                'avg_sentiment': 0,
                'sentiment_trend': 0,
                'news_volume': 0,
            }
        
        features = {
            'avg_sentiment': daily_sentiment['sentiment_score'].mean(),
            'sentiment_volatility': daily_sentiment['sentiment_score'].std(),
            'sentiment_trend': daily_sentiment['sentiment_score'].iloc[-5:].mean() - 
                              daily_sentiment['sentiment_score'].iloc[:5].mean(),
            'news_volume': daily_sentiment['news_count'].mean(),
            'positive_ratio': (daily_sentiment['sentiment_label'] == 'positive').mean(),
        }
        
        return features
    
    def extract_market_features(self, code: str) -> Dict[str, Any]:
        """
        提取市场特征
        
        Args:
            code: 股票代码
        
        Returns:
            市场特征字典
        """
        # 这里可以添加与市场相关的特征
        # 如与大盘的相关性、行业排名等
        
        return {
            'market_beta': 1.0,  # 市场贝塔
            'sector_rank': 0.5,  # 行业排名
        }
    
    def extract_all_features(self, code: str) -> Dict[str, Any]:
        """
        提取所有特征
        
        Args:
            code: 股票代码
        
        Returns:
            完整特征字典
        """
        stock = stock_manager.get_stock_by_code(code)
        
        features = {
            'code': code,
            'name': stock.name if stock else code,
            'extract_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'technical': self.extract_technical_features(code),
            'sentiment': self.extract_sentiment_features(code),
            'market': self.extract_market_features(code),
        }
        
        return features
    
    def analyze_with_ai(self, code: str) -> Dict[str, Any]:
        """
        使用AI分析股票特征
        
        Args:
            code: 股票代码
        
        Returns:
            AI分析结果
        """
        features = self.extract_all_features(code)
        signals = indicator_analyzer.get_latest_signals(code)
        
        # 构建提示
        prompt = f"""
请分析以下股票的技术特征，判断该股票最适合的投资策略类型：

股票代码: {code}
股票名称: {features['name']}

【技术指标】
- RSI(6): {signals.get('rsi_6', 'N/A')}
- MACD柱状图: {signals.get('macd_histogram', 'N/A')}
- 均线趋势: {signals.get('ma_trend', 'N/A')}
- 综合评分: {signals.get('overall_score', 'N/A')}

【技术特征】
- 趋势强度: {features['technical'].get('trend_strength', 'N/A')}
- RSI水平: {features['technical'].get('rsi_level', 'N/A')}
- 布林带位置: {features['technical'].get('bollinger_position', 'N/A')}

【情感特征】
- 平均情感: {features['sentiment'].get('avg_sentiment', 'N/A')}
- 情感趋势: {features['sentiment'].get('sentiment_trend', 'N/A')}

请输出JSON格式的分析结果，包含以下字段：
- strategy_type: 策略类型 (trend_following/value/momentum/swing/mean_reversion)
- confidence: 置信度 (0-1)
- reasoning: 分析理由
- recommended_indicators: 推荐指标列表
- risk_level: 风险等级 (low/medium/high)
- suitable_for: 适合的投资者类型
"""
        
        try:
            response = self.ai_client.call(prompt)
            
            # 尝试解析JSON
            try:
                # 提取JSON部分
                json_match = response[response.find('{'):response.rfind('}')+1]
                ai_result = json.loads(json_match)
            except:
                # 如果解析失败，返回原始文本
                ai_result = {
                    'raw_response': response,
                    'strategy_type': 'unknown',
                    'confidence': 0,
                }
            
            return {
                'features': features,
                'ai_analysis': ai_result,
            }
            
        except Exception as e:
            logger.error(f"AI分析失败 {code}: {e}")
            return {
                'features': features,
                'ai_analysis': {'error': str(e)},
            }
    
    def save_features(self, code: str, features: Dict):
        """保存特征到本地（NaN/Infinity 替换为 None 以确保合法 JSON）"""
        import math
        def _sanitize(obj):
            if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            if isinstance(obj, dict):
                return {k: _sanitize(v) for k, v in obj.items()}
            if isinstance(obj, (list, tuple)):
                return [_sanitize(v) for v in obj]
            return obj
        file_path = self._get_feature_path(code)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(_sanitize(features), f, ensure_ascii=False, indent=2)
    
    def load_features(self, code: str) -> Optional[Dict]:
        """从本地加载特征"""
        file_path = self._get_feature_path(code)
        
        if not os.path.exists(file_path):
            return None
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def extract_all_stocks_features(self) -> Dict[str, Dict]:
        """
        提取所有股票的特征
        
        Returns:
            特征字典
        """
        results = {}
        stocks = stock_manager.get_all_stocks()
        
        for stock in stocks:
            try:
                logger.info(f"正在提取 {stock.name}({stock.code}) 的特征...")
                features = self.analyze_with_ai(stock.code)
                self.save_features(stock.code, features)
                results[stock.code] = features
            except Exception as e:
                logger.error(f"提取 {stock.code} 特征失败: {e}")
        
        return results


class StrategyTypeClassifier:
    """策略类型分类器"""
    
    STRATEGY_TYPES = {
        'trend_following': {
            'name': '趋势跟踪',
            'description': '跟随市场趋势，在上升趋势中买入，下降趋势中卖出',
            'indicators': ['MA', 'MACD', 'ADX'],
            'timeframe': '中期',
        },
        'value': {
            'name': '价值投资',
            'description': '寻找被低估的股票，长期持有',
            'indicators': ['PE', 'PB', 'ROE'],
            'timeframe': '长期',
        },
        'momentum': {
            'name': '动量策略',
            'description': '买入近期表现强势的股票',
            'indicators': ['RSI', 'ROC', 'MOM'],
            'timeframe': '短期',
        },
        'swing': {
            'name': '波段操作',
            'description': '利用价格波动进行短期交易',
            'indicators': ['KDJ', 'Bollinger Bands', 'RSI'],
            'timeframe': '短期-中期',
        },
        'mean_reversion': {
            'name': '均值回归',
            'description': '价格偏离均值时反向操作',
            'indicators': ['Bollinger Bands', 'RSI', 'Z-Score'],
            'timeframe': '短期',
        },
    }
    
    def classify(self, features: Dict) -> Dict:
        """
        根据特征分类策略类型
        
        Args:
            features: 特征字典
        
        Returns:
            分类结果
        """
        tech = features.get('technical', {})
        
        scores = {}
        
        # 趋势跟踪
        trend_score = tech.get('trend_strength', 0) * 0.5 + \
                     (1 - tech.get('volatility_proxy', 0.5)) * 0.5
        scores['trend_following'] = trend_score
        
        # 动量策略
        momentum_score = abs(tech.get('rsi_level', 0.5) - 0.5) * 2
        scores['momentum'] = momentum_score
        
        # 波段操作
        swing_score = tech.get('volatility_proxy', 0.5) * 0.5 + \
                     (1 - abs(tech.get('bollinger_position', 0.5) - 0.5) * 2) * 0.5
        scores['swing'] = swing_score
        
        # 均值回归
        mr_score = 1 - trend_score
        scores['mean_reversion'] = mr_score
        
        # 选择得分最高的策略
        best_strategy = max(scores, key=scores.get)
        
        return {
            'primary_strategy': best_strategy,
            'strategy_info': self.STRATEGY_TYPES.get(best_strategy, {}),
            'confidence': scores[best_strategy],
            'all_scores': scores,
        }


# 全局实例
feature_extractor = FeatureExtractor()
strategy_classifier = StrategyTypeClassifier()
