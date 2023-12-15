import pandas as pd
import numpy as np
import sys
import feather
import time 
from datetime import datetime, timedelta
from mysystem.read import StkDataProcessor

class Backtester:
    def __init__(self, start_date, end_date, initial_capital=10000000, file_path='../data/stk_daily.feather'):
        self.data_processor = StkDataProcessor(file_path)
        self.data_processor.preprocess_data()
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.signals = pd.DataFrame(columns=['date', 'stk_id', 'action', 'volume'])
        self.initial_capital = initial_capital
        self.initialize_account()

    def get_trading_days(self):
        # 获取指定时间范围内的所有交易日
        all_trading_days = pd.to_datetime(self.data_processor.trading_days, format='%Y-%m-%d')
        trading_days = [date for date in all_trading_days if self.start_date <= date <= self.end_date]
        return trading_days
    
    def interpolate_stock_data(self, stock_codes=None):

        # 获取特定股票在特定时间内的行情数据
        stock_data = self.data_processor.get_stock_data(stock_codes, self.start_date, self.end_date)

        # 生成包含所有交易日的时间序列
        trading_days = self.get_trading_days()
        full_trading_days = pd.to_datetime(trading_days, format='%Y-%m-%d')

        # 将数据重新索引为包含所有交易日的时间序列，并进行插值
        #interpolated_data = stock_data.set_index('date').reindex(full_trading_days).interpolate(method='ffill').reset_index()
        interpolated_data = (
        stock_data.groupby(['date', 'stk_id'])
        .apply(lambda group: group.set_index('date').reindex(full_trading_days).interpolate(method='ffill'))
        .reset_index(drop=True)
        )
        
    

        return interpolated_data
    
    def initialize_account(self):
        # 初始化账户状态
        self.current_capital = self.initial_capital
        self.portfolio = {}

        # 获取所有交易日
        all_trading_days = pd.to_datetime(self.get_trading_days(), format='%Y-%m-%d')

        # 初始化net_values，所有交易日的净值设为初始资金
        net_values_data = {'date': all_trading_days, 'net_value': [self.initial_capital] * len(all_trading_days)}
        self.net_values = pd.DataFrame(net_values_data)

    
    def reset_signals(self, file_path=None):
        if file_path is not None:
            # 从文件读取 signals
            self.signals = pd.read_csv(file_path)
        else:
            # 如果 file_path 为 None，则初始化为空的 DataFrame
            self.signals = pd.DataFrame(columns=['date', 'stk_id', 'action', 'volume'])


    def buy(self, date, stock_list, volume=100):
        for stk_id in stock_list:
            new_signal = pd.DataFrame([[date, stk_id, 'buy', volume]], columns=['date', 'stk_id', 'action', 'volume'])
            self.signals = pd.concat([self.signals, new_signal], ignore_index=True)

    def sell(self, date, stock_list, volume=sys.maxsize):
        # volume的默认值是全部卖出
        for stk_id in stock_list:
            new_signal = pd.DataFrame([[date, stk_id, 'sell', volume]], columns=['date', 'stk_id', 'action', 'volume'])
            self.signals = pd.concat([self.signals, new_signal], ignore_index=True)

    def clear(self, date):
        new_signal = pd.DataFrame([[date, None, 'clear', None]], columns=['date', 'stk_id', 'action', 'volume'])
        self.signals = pd.concat([self.signals, new_signal], ignore_index=True)

    def calculate_daily_net_value(self, date, current_capital, portfolio):
        # 获取当天的净值数据
        net_value = current_capital
        stk_data = self.stk_data
        # 计算持仓股票市值并累加到净值中
        for stk_id, position in portfolio.items():
            # 获取当天的股价，如果当天没有行情，则取最近有行情的一天的价格
            current_date_price = stk_data.loc[(stk_data['stk_id'] == stk_id) & (stk_data['date'] == date), 'adj_close'].values
            #print(type(date))
            if not current_date_price:
                # 当天没有行情，取最近有行情的一天的价格
                last_trading_day = stk_data.loc[stk_data['date'] < date].max()['date']
                current_date_price = stk_data.loc[(stk_data['stk_id'] == stk_id) & (stk_data['date'] == last_trading_day), 'adj_close'].values
            current_price = current_date_price[0]

            # 计算持仓股票的市值并累加到净值中
            stock_value = position * current_price
            net_value += stock_value

        return net_value
    # def calculate_daily_net_value(self, date, current_capital, portfolio):
    #     # 获取当天的净值数据
    #     net_value = current_capital
    #     stk_data = self.stk_data

    #     # 获取所有持仓股票的行情数据
    #     portfolio_data = stk_data[(stk_data['stk_id'].isin(portfolio.keys())) & (stk_data['date'] == date)]

    #     # 计算持仓股票市值并累加到净值中
    #     stock_values = portfolio_data['adj_close'] * portfolio_data['stk_id'].map(portfolio)
    #     net_value += stock_values.sum()

    #     return net_value



    def run_backtest(self):
        self.stk_data = self.data_processor.get_stock_data(start_date=self.start_date, end_date=self.end_date)
        #self.stk_data = self.interpolate_stock_data()
        self.stk_data['date'] = pd.to_datetime(self.stk_data['date'], format='%Y-%m-%d').dt.date
        stk_data = self.stk_data
        last_signal_date = self.start_date
        self.signals['date'] = pd.to_datetime(self.signals['date'], format='%Y-%m-%d')
        

        for _, signal in self.signals.iterrows():
            date = signal['date']
            stk_id = signal['stk_id']
            action = signal['action']
            volume = signal['volume']
            print(f"{date},{stk_id},{action}")
            t0 = time.time()

            # 更新last_signal_date到date之间的净值数据
            # 判断 date 是否在 net_values 的 date 列里面
            if date not in self.net_values['date'].values:
                print(f"Error: {date}.")
                continue

            # 获取 last_signal_date 到 date 之间的所有日期
            date_range = self.net_values.loc[(self.net_values['date'] > last_signal_date) & (self.net_values['date'] <= date), 'date']
            # 遍历日期，调用 calculate_daily_net_value 计算净值数据，并填入 net_values 对应的每一行里面
            for update_date in date_range:
                #update_date = update_date.date()

                # 调用 calculate_daily_net_value 计算净值数据
                daily_net_value = self.calculate_daily_net_value(update_date, self.current_capital, self.portfolio)

                # 填入 net_values对应的每一行里面
                update_index = self.net_values[self.net_values['date'] == update_date].index[0]
                self.net_values.loc[update_index, 'net_value'] = daily_net_value
            last_signal_date = date
            # 根据交易信号执行相应操作
            if action == 'buy':
                # 判断 stk_id 和 date 是否在同一行中
                if not stk_data.loc[(stk_data['stk_id'] == stk_id) & (stk_data['date'] == date)].empty:
                    # 执行买入操作的逻辑
                    stock_price = stk_data.loc[(stk_data['stk_id'] == stk_id) & (stk_data['date'] == date), 'adj_close'].values[0]
                    cost = volume * stock_price
                    if self.current_capital >= cost:
                        # 可以正常执行交易
                        self.current_capital -= cost
                        self.portfolio[stk_id] = self.portfolio.get(stk_id, 0) + volume
                    else:
                        # 资金不足，无法买入
                        print(f"Insufficient funds to buy {volume} shares of {stk_id} on {date}.")
                else:
                    # 股票当天不能交易
                    print(f"{stk_id} is not available for trading on {date}.")
                
            elif action == 'sell':
                # 获取当前股票的持仓数量
                current_position = self.portfolio.get(stk_id, 0)
                
                # 计算实际卖出的数量，取 volume 和当前持仓的较小值
                sell_quantity = min(volume, current_position)
                
                if sell_quantity > 0:
                    # 获取卖出当天的股价，如果当天没有行情，则取最近有行情的一天的价格
                    sell_date_price = stk_data.loc[(stk_data['stk_id'] == stk_id) & (stk_data['date'] == date), 'adj_close'].values
                    if not sell_date_price:
                        # 当天没有行情，取最近有行情的一天的价格
                        last_trading_day = stk_data.loc[stk_data['date'] < date].max()['date']
                        sell_date_price = stk_data.loc[(stk_data['stk_id'] == stk_id) & (stk_data['date'] == last_trading_day), 'adj_close'].values
                    sell_price = sell_date_price[0]
                    
                    # 计算卖出的总价值
                    sell_value = sell_quantity * sell_price
                    
                    # 更新当前资金和持仓
                    self.current_capital += sell_value
                    self.portfolio[stk_id] -= sell_quantity
                    if self.portfolio[stk_id] == 0:
                        del self.portfolio[stk_id]
                    
                    # 输出卖出信息
                    #print(f"Sold {sell_quantity} shares of {stk_id} on {date} at a price of {sell_price}.")

            elif action == 'clear':
                # 遍历当前持仓的所有股票
                for stk_id, current_position in self.portfolio.items():
                    if current_position > 0:
                        # 获取清仓当天的股价，如果当天没有行情，则取最近有行情的一天的价格
                        clear_date_price = stk_data.loc[(stk_data['stk_id'] == stk_id) & (stk_data['date'] == date), 'adj_close'].values
                        if not clear_date_price:
                            # 当天没有行情，取最近有行情的一天的价格
                            last_trading_day = stk_data.loc[stk_data['date'] < date].max()['date']
                            clear_date_price = stk_data.loc[(stk_data['stk_id'] == stk_id) & (stk_data['date'] == last_trading_day), 'adj_close'].values
                        clear_price = clear_date_price[0]
                        
                        # 计算清仓的总价值
                        clear_value = current_position * clear_price
                        
                        # 更新当前资金和持仓
                        self.current_capital += clear_value
                        
                        
                        # 输出清仓信息
                        #print(f"Cleared position of {current_position} shares of {stk_id} on {date} at a price of {clear_price}.")
                self.portfolio = {}

            else:
                print(f"Error: Invalid action type '{action}'.")

        # 获取 last_signal_date 到 end_date 之间的所有日期
        date_range = self.net_values.loc[(self.net_values['date'] > last_signal_date) & (self.net_values['date'] <= self.end_date), 'date']
        # 遍历日期，调用 calculate_daily_net_value 计算净值数据，并填入 net_values 对应的每一行里面
        for update_date in date_range:
            #update_date = update_date.date()

            # 调用 calculate_daily_net_value 计算净值数据
            daily_net_value = self.calculate_daily_net_value(update_date, self.current_capital, self.portfolio)

            # 填入 net_values对应的每一行里面
            update_index = self.net_values[self.net_values['date'] == update_date].index[0]
            self.net_values.loc[update_index, 'net_value'] = daily_net_value
        
        self.net_values.to_csv('net_values.csv', index=False)
        return self.net_values
            

        
        
