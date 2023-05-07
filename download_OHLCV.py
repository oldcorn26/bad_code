import logging

import aiohttp
import ccxt.async_support as ccxt
import pandas as pd
import pickle
import asyncio
import time

from aiohttp_socks import ProxyConnector

start = 1577808000000
fail_symbols = []

# 定义协程函数，用于获取一个币对的指定时间段内的OHLCV数据并存储到pkl文件中
async def fetch_ohlcv(exchange, symbol):
    end = exchange.milliseconds()
    fail_times = 0
    while fail_times < 3:
        try:
            # 初始化一个空的dataframe，用于存储所有的OHLCV数据
            df_all = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            # 计算当前币对获取数据的时间段范围
            current_start = start
            i = 0
            while i < 10:
                # 获取当前时间段内的OHLCV数据，并将其添加到总的dataframe中
                ohlcv = await exchange.fetch_ohlcv(symbol, '1h', current_start, 1000)
                df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                # df_all = df_all.append(df, ignore_index=True)
                df_all = pd.concat([df_all, df], ignore_index=True)
                # 更新时间段范围
                current_start = ohlcv[-1][0] + 3600000
                time.sleep(1)
                i += 1
            # 将总的dataframe存储到以币对名命名的pkl文件中
            df_all['timestamp'] = pd.to_datetime(df_all['timestamp'], unit='ms')
            df_all.set_index('timestamp', inplace=True)
            with open(f"{symbol}.pkl", 'wb') as f:
                pickle.dump(df_all, f)
            # 打印当前币对获取数据的成功信息
            print(f"Successfully fetched {symbol} data")
        except:
            # 打印当前币对获取数据的失败信息
            print(f"Failed to fetch {symbol} data")
            time.sleep(5)
            fail_times += 1
    if fail_times == 3:
        print(f"Failed to fetch {symbol} data after 3 times")
        fail_symbols.append(symbol)


# 定义协程函数，用于获取所有币对的指定时间段内的OHLCV数据并存储到对应的pkl文件中
async def fetch_all_ohlcv(exchange):

    # 获取交易所支持的所有币对
    symbols = await exchange.load_markets()
    symbols_list = [symbol for symbol in symbols.keys() if symbol.endswith('USDT') and 'DOWN' not in symbol]
    # 创建一个协程对象列表，用于存储所有的协程对象
    tasks = []
    # 循环遍历所有币对，根据时间间隔获取对应时间段内的OHLCV数据，并创建对应的协程对象
    for symbol in symbols_list:
        tasks.append(fetch_ohlcv(exchange, symbol))
    # 并发运行所有协程对象，并等待所有协程完成
    await asyncio.gather(*tasks)


async def main():
    # 初始化Binance交易所
    exchange = ccxt.binance()
    connector = ProxyConnector.from_url('socks5://127.0.0.1:1081')
    session = aiohttp.ClientSession(connector=connector)
    exchange.session = session
    exchange.timeout = 30000
    # exchange.enableRateLimit = True

    await fetch_all_ohlcv(exchange)
    logging.info(f"Failed symbols: {fail_symbols}")
    await exchange.close()

asyncio.run(main())
