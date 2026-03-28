"""
新闻采集与情感分析模块
从新浪财经采集股票新闻并进行情感分析
"""

import os
import re
import json
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

from .config_manager import config
from .stock_manager import stock_manager

logger = logging.getLogger(__name__)


class NewsCollector:
    """新闻采集器"""
    
    def __init__(self):
        self.data_dir = config.get('data_storage.news_dir', './data/news')
        self.track_days = config.get('data_collection.news.track_days', 30)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self._ensure_dir()
    
    def _ensure_dir(self):
        """确保数据目录存在"""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
    
    def _get_news_path(self, code: str) -> str:
        """获取新闻存储路径（code 为后缀格式如 600500.SH → 600500_SH_news.csv）"""
        safe_code = code.replace('.', '_')
        return os.path.join(self.data_dir, f"{safe_code}_news.csv")
    
    def fetch_stock_news(self, code: str, max_pages: int = 10) -> pd.DataFrame:
        """
        获取对应股票近N个月的新闻
        
        Args:
            code: 股票代码，后缀格式如 '600519.SH'、'000001.SZ'
            max_pages: 最大翻页数
        
        Returns:
            包含新闻数据的 DataFrame
        """
        # 获取股票信息
        stock = stock_manager.get_stock_by_code(code)
        if not stock:
            logger.error(f"未知的股票代码: {code}")
            return pd.DataFrame()
        
        # 计算日期范围
        end_date = datetime.today()
        start_date = end_date - timedelta(days=self.track_days)
        end_date_str = end_date.strftime('%Y-%m-%d')
        start_date_str = start_date.strftime('%Y-%m-%d')
        
        news_list = []
        page = 1
        
        while page <= max_pages:
            # 构建新浪财经股票新闻 URL
            # 使用带市场前缀的代码格式，这是新浪新闻页面的要求
            sina_code = stock.prefix_code  # 如 sh600519, sz000001
            
            if page == 1:
                url = f'https://vip.stock.finance.sina.com.cn/corp/go.php/vCB_AllNewsStock/symbol/{sina_code}.phtml'
            else:
                url = f'https://vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php?symbol={sina_code}&Page={page}'
            
            try:
                logger.info(f"正在获取 {stock.name}({code}) 第 {page} 页新闻...{url}")
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                response.encoding = 'gb2312'
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 查找个股相关资讯部分
                datelist = soup.find('div', class_='datelist')
                if not datelist:
                    logger.warning(f"第 {page} 页未找到新闻数据")
                    break
                
                ul_tag = datelist.find('ul')
                if not ul_tag:
                    logger.warning(f"第 {page} 页未找到新闻列表")
                    break
                
                found_news = False
                stop_fetching = False
                
                for item in ul_tag.find_all('a'):
                    # 获取 a 标签前的文本（日期和时间）
                    prev_text = item.previous_sibling
                    if prev_text:
                        #logger.info("prev_text", prev_text)
                        prev_text = prev_text.strip()  # 关键：使用strip()处理空白字符
                    else:
                        continue
                    
                    # 解析日期和时间
                    # 格式可能是 "2026-03-15 11:51" 或 "2026-03-15"
                    parts = [prev_text[:10], prev_text[11:]]
                    if len(parts) >= 1:
                        news_date = parts[0]
                        news_time = parts[1] if len(parts) > 1 else '00:00'
                        news_text = item.text.strip()
                        news_url = item.get('href', '')
                        logger.debug(f"提取到新闻: 日期={news_date}, 时间={news_time}, 标题={news_text}")
                        
                        # 检查新闻日期是否在规定范围内
                        # 使用字符串比较，与参考实现保持一致
                        if start_date_str <= news_date <= end_date_str:
                            news_list.append({
                                'code': stock.full_code,
                                'name': stock.name,
                                'date': news_date,
                                'time': news_time,
                                'title': news_text,
                                'url': news_url,
                                'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            })
                            found_news = True
                        elif news_date < start_date_str:
                            # 如果新闻日期早于开始日期，停止翻页
                            stop_fetching = True
                            break
                if not found_news or stop_fetching:
                    # 如果当前页没有找到符合日期范围的新闻，停止翻页
                    break
                    
            except requests.RequestException as e:
                logger.error(f"请求出错: {e}")
                break
            except Exception as e:
                logger.error(f"发生未知错误: {e}")
                break
            
            page += 1
        
        news_df = pd.DataFrame(news_list)
        
        # 保存到本地
        if not news_df.empty:
            self._save_news(stock.full_code, news_df)

        logger.info(f"成功获取 {stock.name}({code}) {len(news_df)} 条新闻")
        return news_df
    
    def _save_news(self, code: str, df: pd.DataFrame):
        """保存新闻到本地"""
        file_path = self._get_news_path(code)
        
        # 如果文件已存在，合并数据
        if os.path.exists(file_path):
            old_df = pd.read_csv(file_path)
            df = pd.concat([old_df, df], ignore_index=True)
            df = df.drop_duplicates(subset=['date', 'title'], keep='last')
        
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
    
    def fetch_all_stocks_news(self) -> Dict[str, pd.DataFrame]:
        """
        获取所有配置股票的新闻
        
        Returns:
            字典，key为代码，value为DataFrame
        """
        stocks = stock_manager.get_all_stocks()
        results = {}
        
        for stock in stocks:
            try:
                df = self.fetch_stock_news(stock.full_code)
                results[stock.full_code] = df
            except Exception as e:
                logger.error(f"获取 {stock.name}({stock.code}) 新闻失败: {e}")
        
        return results
    
    def load_news(self, code: str) -> pd.DataFrame:
        """
        加载本地存储的新闻，code 可以是后缀格式或裸码
        
        Args:
            code: 股票代码（如 '600519.SH' 或 '600519'）
        
        Returns:
            DataFrame
        """
        # 优先尝试直接以 code 解析为文件名
        file_path = self._get_news_path(code)
        if os.path.exists(file_path):
            return pd.read_csv(file_path)
        # 如果没有找到，尝试用 stock_manager 解析为 full_code
        try:
            stock = stock_manager.get_stock_by_code(code)
            if stock and stock.full_code:
                alt_path = self._get_news_path(stock.full_code)
                if os.path.exists(alt_path):
                    return pd.read_csv(alt_path)
        except Exception:
            pass
        return pd.DataFrame()


class SentimentAnalyzer:
    """情感分析器"""
    
    def __init__(self):
        # 默认使用本地情感分析，避免网络依赖
        self.model_type = config.get('data_collection.news.sentiment_model', 'local')
        self.token = config.get_modelscope_token()
        
        # 如果没有有效的token，强制使用本地分析
        if not self.token or self.model_type == 'local':
            self.model_type = 'local'
            logger.debug("使用本地情感分析（无网络依赖）")
        else:
            logger.debug("使用ModelScope在线情感分析")
    
    def analyze(self, text: str) -> Dict:
        """
        分析文本情感
        
        Args:
            text: 待分析的文本
        
        Returns:
            包含情感分数和标签的字典
        """
        if self.model_type == 'modelscope':
            return self._analyze_with_modelscope(text)
        else:
            return self._analyze_with_local(text)
    
    def _analyze_with_modelscope(self, text: str) -> Dict:
        """使用ModelScope API进行情感分析"""
        try:
            import http.client
            import urllib.parse
            import socket
            
            # 设置连接超时，避免长时间等待
            conn = http.client.HTTPSConnection("api.modelscope.cn", timeout=5)
            
            payload = json.dumps({
                "input": {
                    "text": text
                }
            })
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            conn.request("POST", "/api/v1/studio/damo/nlp_structbert_sentiment-classification_chinese-base/gradio/api/predict", 
                        payload, headers)
            
            res = conn.getresponse()
            data = res.read()
            result = json.loads(data.decode("utf-8"))
            
            # 解析结果
            if 'data' in result:
                sentiment_data = result['data']
                return {
                    'score': sentiment_data.get('score', 0),
                    'label': sentiment_data.get('label', 'neutral'),
                    'positive_prob': sentiment_data.get('positive_prob', 0.5),
                    'negative_prob': sentiment_data.get('negative_prob', 0.5),
                }
            else:
                # API返回但无有效数据，使用本地分析
                logger.debug("ModelScope API返回无效数据，使用本地分析")
                return self._analyze_with_local(text)
                
        except Exception as e:
            # 网络类错误时提示并回退到本地分析
            err_no = getattr(e, 'errno', None)
            if isinstance(e, OSError) and err_no in (11001,):
                logger.warning(f"ModelScope 网络错误 (errno={err_no})，情感分析将使用本地模式: {e}")
                self.model_type = 'local'
                return self._analyze_with_local(text)
            # 其他错误退到本地分析但记录为 debug
            logger.debug(f"ModelScope情感分析暂时不可用: {str(e)[:100]}...")
            return self._analyze_with_local(text)
        finally:
            try:
                conn.close()
            except:
                pass
    
    def _analyze_with_local(self, text: str) -> Dict:
        """
        使用本地规则进行情感分析
        
        基于关键词的情感分析，简单但有效
        """
        # 正面关键词
        positive_words = [
            '上涨', '涨停', '利好', '增长', '盈利', '突破', '创新高', '买入', '增持',
            '推荐', '看好', '强劲', '反弹', '回升', '改善', '超预期', '分红', '送转',
            '合作', '订单', '中标', '获批', '认证', '专利', '技术突破', '市场份额',
            'positive', 'rise', 'gain', 'profit', 'growth', 'bullish', 'buy'
        ]
        
        # 负面关键词
        negative_words = [
            '下跌', '跌停', '利空', '亏损', '下降', '跌破', '创新低', '卖出', '减持',
            '回避', '看空', '疲软', '回调', '回落', '恶化', '低于预期', '退市', '风险',
            '调查', '处罚', '诉讼', '债务', '违约', '裁员', '停产', '事故', 'negative',
            'fall', 'loss', 'decline', 'bearish', 'sell'
        ]
        
        try:
            text_lower = str(text).lower()
        except Exception:
            text_lower = ''

        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        total = positive_count + negative_count
        
        if total == 0:
            return {
                'score': 0,
                'label': 'neutral',
                'positive_prob': 0.5,
                'negative_prob': 0.5,
            }
        
        positive_prob = positive_count / total
        negative_prob = negative_count / total
        
        # 计算情感分数 (-1 到 1)
        score = (positive_count - negative_count) / max(total, 1)
        
        # 确定标签
        if score > 0.2:
            label = 'positive'
        elif score < -0.2:
            label = 'negative'
        else:
            label = 'neutral'
        
        return {
            'score': round(score, 4),
            'label': label,
            'positive_prob': round(positive_prob, 4),
            'negative_prob': round(negative_prob, 4),
        }
    
    def analyze_news_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        分析DataFrame中所有新闻的情感
        
        Args:
            df: 包含新闻的DataFrame
        
        Returns:
            添加了情感分析列的DataFrame
        """
        if df.empty:
            return df
        
        sentiments = []
        for title in df['title']:
            result = self.analyze(title)
            sentiments.append(result)
        
        # 添加情感分析结果列
        df['sentiment_score'] = [s['score'] for s in sentiments]
        df['sentiment_label'] = [s['label'] for s in sentiments]
        df['positive_prob'] = [s['positive_prob'] for s in sentiments]
        df['negative_prob'] = [s['negative_prob'] for s in sentiments]
        
        return df
    
    def analyze_stock_news(self, code: str) -> pd.DataFrame:
        """
        分析某只股票所有新闻的情感
        
        Args:
            code: 股票代码（可为裸码或后缀码）
        
        Returns:
            添加了情感分析的DataFrame
        """
        collector = NewsCollector()
        # 尝试解析为 full_code
        stock = stock_manager.get_stock_by_code(code)
        code_to_use = stock.full_code if stock else code
        df = collector.load_news(code_to_use)
        
        if df.empty:
            logger.warning(f"未找到 {code_to_use} 的新闻数据")
            return df
        
        return self.analyze_news_df(df)
    
    def get_daily_sentiment(self, code: str) -> pd.DataFrame:
        """
        获取股票每日情感汇总
        
        Args:
            code: 股票代码
        
        Returns:
            按日期汇总的情感数据
        """
        df = self.analyze_stock_news(code)
        
        if df.empty:
            return df
        
        # 按日期分组汇总
        daily = df.groupby('date').agg({
            'sentiment_score': 'mean',
            'positive_prob': 'mean',
            'negative_prob': 'mean',
            'title': 'count',
        }).rename(columns={'title': 'news_count'})
        
        daily['sentiment_label'] = daily['sentiment_score'].apply(
            lambda x: 'positive' if x > 0.2 else ('negative' if x < -0.2 else 'neutral')
        )
        
        return daily.reset_index()


class NewsSentimentPipeline:
    """新闻情感分析流水线"""
    
    def __init__(self):
        self.collector = NewsCollector()
        self.analyzer = SentimentAnalyzer()
    
    def run(self, code: Optional[str] = None, save: bool = True) -> Dict[str, pd.DataFrame]:
        """
        运行完整的新闻采集和情感分析流程
        
        Args:
            code: 股票代码，如果为None则处理所有股票
            save: 是否保存结果
        
        Returns:
            结果字典
        """
        results = {}
        
        if code:
            stocks = [stock_manager.get_stock_by_code(code)]
        else:
            stocks = stock_manager.get_all_stocks()
        
        for stock in stocks:
            if not stock:
                continue
            
            try:
                logger.info(f"处理 {stock.name}({stock.full_code}) 的新闻...")
                
                # 1. 采集新闻
                news_df = self.collector.fetch_stock_news(stock.full_code)
                
                if news_df.empty:
                    continue
                
                # 2. 情感分析
                analyzed_df = self.analyzer.analyze_news_df(news_df)
                
                # 3. 保存结果
                if save:
                    safe_code = stock.full_code.replace('.', '_')
                    file_path = os.path.join(
                        config.get('data_storage.news_dir'),
                        f"{safe_code}_news_analyzed.csv"
                    )
                    analyzed_df.to_csv(file_path, index=False, encoding='utf-8-sig')
                
                results[stock.full_code] = analyzed_df
                
                logger.info(f"完成 {stock.name}({stock.full_code}) 的新闻情感分析")
                
            except Exception as e:
                logger.error(f"处理 {stock.full_code} 失败: {e}")
        
        return results


# 全局实例
news_collector = NewsCollector()
sentiment_analyzer = SentimentAnalyzer()
news_pipeline = NewsSentimentPipeline()
