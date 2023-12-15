## 一个简单的量化交易回测系统

- mysystem/内包含此系统的三个模块：read, backtest, output

- 默认需要的数据为data/stk_daily.feather，即股票日行情数据。基准收益率使用了newdata/上证A股指数历史数据.csv

- test.ipynb展示了使用此系统的基本流程：

  1. 导入模块

     ```
     from mysystem.read import StkDataProcessor
     from mysystem.backtest import Backtester
     from mysystem.output import PerformanceCalculator
     ```

  2. 设置回测开始和结束时间，设置初始资金

     ```
     backtester = Backtester(start_date='2021-01-01', end_date='2022-01-01',initial_capital=10000000)
     ```

  3. 利用backtester中的buy, sell, clear函数，或手动生成交易信号backtester.signals

     ```
     backtester.buy(date=current_date, stock_list=stock_list, volume=volume)
     ```

  4. 开始回测，系统计算整个回测过程中账户的净值数据

     ```
     backtester.initialize_account()
     net_values = backtester.run_backtest() # 运行结束会自动生成net_values.csv并保存
     ```

  5. 利用output模块计算此次回测的performance数据，会生成净值曲线图，每日收益率、基准收益率和超额收益率曲线图，并输出年化收益，年化波动，夏普比率，最大回撤

     ```
     performance_calculator = PerformanceCalculator()
     performance_result = performance_calculator.calculate_performance(net_values)
     ```

     