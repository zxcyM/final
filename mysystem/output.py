import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class PerformanceCalculator:
    def __init__(self, benchmark_data_path='newdata/上证A股指数历史数据.csv', risk_free_rate=0, trading_days_per_year=252):
        self.benchmark_data_path = benchmark_data_path
        self.risk_free_rate = risk_free_rate
        self.trading_days_per_year = trading_days_per_year

    def calculate_daily_returns(self, net_values):
        # 计算每日收益率
        net_values['daily_returns'] = net_values['net_value'].pct_change()
        net_values['daily_returns'].fillna(0, inplace=True)  # 将NaN替换为0
        return net_values

    def calculate_excess_returns(self, net_values, benchmark_returns):
        # 计算超额收益
        net_values['excess_returns'] = net_values['daily_returns'] - benchmark_returns
        return net_values

    def calculate_annualized_returns(self, net_values):
        # 计算年化收益率
        total_returns = (net_values['net_value'].iloc[-1] / net_values['net_value'].iloc[0]) - 1
        annualized_returns = (1 + total_returns) ** (self.trading_days_per_year / len(net_values)) - 1
        return annualized_returns

    def calculate_annualized_volatility(self, net_values):
        # 计算年化波动率
        annualized_volatility = net_values['daily_returns'].std() * np.sqrt(self.trading_days_per_year)
        return annualized_volatility

    def calculate_sharpe_ratio(self, net_values):
        # 计算夏普比率
        excess_returns = net_values['excess_returns'].mean()
        volatility = net_values['daily_returns'].std() * np.sqrt(self.trading_days_per_year)
        sharpe_ratio = (excess_returns - self.risk_free_rate) / volatility
        return sharpe_ratio

    def calculate_max_drawdown(self, net_values):
        # 计算最大回撤
        peak = net_values['net_value'].cummax()
        drawdown = (net_values['net_value'] - peak) / peak
        max_drawdown = drawdown.min()
        return max_drawdown

    def calculate_performance(self, net_values):
        # 读取基准数据
        benchmark_data = pd.read_csv(self.benchmark_data_path, converters={'returns': lambda x: float(x.strip('%')) / 100})
        net_values['date'] = pd.to_datetime(net_values['date'])
        benchmark_data['date'] = pd.to_datetime(benchmark_data['date'])
        
        merged_data = pd.merge(net_values, benchmark_data[['date', 'returns']], on='date', how='left')
        benchmark_returns = merged_data['returns'].values

        # 计算各项性能指标
        net_values = self.calculate_daily_returns(net_values)
        net_values = self.calculate_excess_returns(net_values, benchmark_returns)
        annualized_returns = self.calculate_annualized_returns(net_values)
        annualized_volatility = self.calculate_annualized_volatility(net_values)
        sharpe_ratio = self.calculate_sharpe_ratio(net_values)
        max_drawdown = self.calculate_max_drawdown(net_values)
        self.plot_net_value_curve(net_values, benchmark_returns)

        return {
            'annualized_returns': annualized_returns,
            'annualized_volatility': annualized_volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown
        }
    
    def plot_net_value_curve(self, net_values, benchmark_returns):
        # 绘制净值曲线
        plt.figure(figsize=(12, 6))
        plt.subplot(2, 1, 1)
        plt.plot(net_values['date'], net_values['net_value'], label='Net Value', color='blue')
        plt.title('Net Value Curve')
        plt.xlabel('Date')
        plt.ylabel('Net Value')
        plt.legend()

        # 绘制每日收益率、基准收益率和超额收益
        plt.subplot(2, 1, 2)
        plt.plot(net_values['date'], net_values['daily_returns'], label='Daily Returns', color='green')
        
        plt.plot(net_values['date'], benchmark_returns, label='Benchmark Returns', color='red')
        
        plt.plot(net_values['date'], net_values['excess_returns'], label='Excess Returns', color='purple')

        plt.title('Daily Returns, Benchmark Returns, and Excess Returns')
        plt.xlabel('Date')
        plt.ylabel('Returns')
        plt.legend()

        plt.tight_layout()
        plt.show()


