import pandas as pd
import feather
import numpy as np
from datetime import datetime

class StkDataProcessor:
    def __init__(self, file_path='../data/stk_daily.feather'):
        self.df = feather.read_dataframe(file_path)
        self.df['date'] = pd.to_datetime(self.df['date'], format='%Y-%m-%d')
        # 统计股票ID
        self.stock_ids = self.df['stk_id'].unique().tolist()
        # 统计交易日
        self.trading_days = self.df['date'].unique()

        # 统计数据的起止时间
        self.start_date = self.df['date'].min()
        self.end_date = self.df['date'].max()
        self.processed = False

    def preprocess_data(self):
        # 计算复权价和日收益率
        self.df = self.df.sort_values(by=['stk_id', 'date'])
        self.df['adj_close'] = self.df['close'] * self.df['cumadj']
        self.df['return'] = (self.df['adj_close'] - self.df['adj_close'].shift(1)) / self.df['adj_close'].shift(1) * 100
        self.processed = True
    
    
    def get_stock_data(self, stock_codes=None, start_date=None, end_date=None):
        # 获取特定股票在特定时间内的行情数据
        if start_date:
            start_date = pd.to_datetime(start_date)
        if end_date:
            end_date = pd.to_datetime(end_date)
        mask = True
        if stock_codes:
            mask &= self.df['stk_id'].isin(stock_codes)
        if start_date:
            mask &= self.df['date'] >= start_date
        if end_date:
            mask &= self.df['date'] <= end_date
        if mask is True:
            return self.df
        else:
            return self.df[mask]
    
    def count_trading_days(self, stock_codes=None, start_date=None, end_date=None):

        if start_date:
            start_date = pd.to_datetime(start_date)
        if end_date:
            end_date = pd.to_datetime(end_date)

        selected_data = self.get_stock_data(stock_codes=stock_codes, start_date=start_date, end_date=end_date)
        trading_days_count = selected_data.groupby('stk_id')['date'].count().to_dict()
        total_trading_days_count = np.sum((self.trading_days >= start_date) & (self.trading_days <= end_date))
        suspended_days_count = {stock_id: total_trading_days_count - count for stock_id, count in trading_days_count.items()}
        return trading_days_count, suspended_days_count, total_trading_days_count

        
    
    def calculate_cumulative_returns(self, stock_codes=None, start_date=None, end_date=None, ascending=True, top_n=None):
        if not self.processed:
            self.preprocess_data()

        # 获取特定股票和时间段的数据
        selected_data = self.get_stock_data(stock_codes=stock_codes, start_date=start_date, end_date=end_date)

        # 初始化一个空列表，用于保存每个股票的结果
        cumulative_returns_list = []

        for stock_id, stock_data in selected_data.groupby('stk_id'):
            cumulative_return_last_day = 100 * ((1 + stock_data['return'] / 100).prod() - 1)
            last_row = pd.DataFrame([[stock_data['date'].max(), stock_id, cumulative_return_last_day]],
                                    columns=['date', 'stk_id', 'cumulative_returns'])
            cumulative_returns_list.append(last_row)

        # 将列表转为 DataFrame
        cumulative_returns_df = pd.concat(cumulative_returns_list, ignore_index=True)

        # 按照收益率升序排序
        cumulative_returns_df = cumulative_returns_df.sort_values(by='cumulative_returns', ascending=ascending)

        # 如果指定了 top_n，只输出前n行
        if top_n is not None:
            cumulative_returns_df = cumulative_returns_df.head(top_n)

        return cumulative_returns_df



