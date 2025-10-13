#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import yfinance as yf
import numpy as np
import time
import logging
import json
import os

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockScanner:
    def __init__(self, delay=1):
        self.delay = delay
        self.results = []
        self.dir_path = os.path.dirname(os.path.abspath(__file__))
        
    def calculate_ema(self, prices, period):
        return prices.ewm(span=period, adjust=False).mean()
    
    def analyze_stock(self, symbol):
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period='1y')
            
            if hist.empty or len(hist) < 120:
                logger.warning(f"{symbol}: 数据不足")
                return None
            
            # 计算EMA
            close_prices = hist['Close']
            ema_30 = self.calculate_ema(close_prices, 30).iloc[-1]
            ema_60 = self.calculate_ema(close_prices, 60).iloc[-1]
            ema_120 = self.calculate_ema(close_prices, 120).iloc[-1]
            
            last_low = hist['Low'].iloc[-1]
            last_high = hist['High'].iloc[-1]
            
            # 检查条件
            condition_1 = ema_30 > ema_60 > ema_120
            condition_2 = (last_low < ema_30) and (last_high > ema_120)
            
            if condition_1 and condition_2:
                logger.info(f"✓ {symbol}: EMA30({ema_30:.2f}) > EMA60({ema_60:.2f}) > EMA120({ema_120:.2f}), Last Low: {last_low}")
                return symbol
            else:
                return None
            
        except Exception as e:
            logger.error(f"分析 {symbol} 时出错: {e}")
            return None
        
        finally:
            time.sleep(self.delay)
    
    def scan_stocks(self):
        logger.info("开始扫描股票...")
        
        # 获取股票列表
        ticker_path = os.path.join(self.dir_path, 'final_tickers.json')
        with open(ticker_path, 'r') as f:
            symbols = json.load(f)
        
        logger.info(f"准备分析 {len(symbols)} 只股票")
        completed = 0
        for symbol in symbols:
            try:
                result = self.analyze_stock(symbol)
                if result:
                    self.results.append(result)
                completed += 1
                if completed % 50 == 0:
                    logger.info(f"已完成 {completed}/{len(symbols)} 只股票的分析")
            except Exception as e:
                logger.error(f"处理 {symbol} 结果时出错: {e}")
        
        logger.info(f"扫描完成，共分析了 {len(self.results)} 只股票")
    
    def save_results(self):
        full_path = os.path.join(self.dir_path, 'full_scan_result.json')
        if os.path.isfile(full_path):
            with open(full_path, 'r') as f:
                old_results = json.load(f)
        else:
            old_results = []
        with open(full_path, 'w') as f:
            json.dump(self.results, f)
        logger.info(f"完整结果已保存到 {full_path}")
        
        delta_path = os.path.join(self.dir_path, 'delta_scan_result.json')
        delta_results = list(set(self.results) - set(old_results))
        with open(delta_path, 'w') as f:
            json.dump(delta_results, f)
        logger.info(f'今日新增结果已保存到 {delta_path}')

    def send_results(self):
        import asyncio
        import telegram
        from dotenv import load_dotenv

        load_dotenv()
        bot_token = os.getenv('BOT_TOKEN')
        chat_id = os.getenv('CHAT_ID')
        bot = telegram.Bot(bot_token)

        delta_path = os.path.join(self.dir_path, 'delta_scan_result.json')
        with open(delta_path, 'r') as f:
            result = str(json.load(f))
        asyncio.run(bot.send_message(chat_id=chat_id, text=result))

def main():
    scanner = StockScanner(delay=1)
    
    try:
        scanner.scan_stocks()

        scanner.save_results()
        logger.info('扫描并保存完毕！')

        scanner.send_results()
        logger.info('结果发送成功！')
        
    except KeyboardInterrupt:
        logger.info("用户中断了扫描")
    except Exception as e:
        logger.error(f"扫描过程中出错: {e}")


if __name__ == "__main__":
    main()

