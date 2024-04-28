import warnings
warnings.filterwarnings("ignore")

import tushare as ts
# print(ts.__version__)
import os
import pandas as pd
import time
from tqdm import tqdm

# params
# root_dir = os.getcwd()
# save_data_dir = os.path.join(root_dir,"data/download_qtq_3year")
start_date = '20240105'
end_date = '20240422'
save_stock_list_dir = '/Users/shiting.wang/.qlib/tushare_data/'
save_data_dir = save_stock_list_dir+start_date+"to"+end_date

# token设置
token = 'f51bc1209389d9ba0d671689478db838d0e903f3c45e3734438612e9'
ts.set_token(token)
pro = ts.pro_api()

if not os.path.exists(save_data_dir):
    os.makedirs(save_data_dir)

# 获取 A 股所有股票的列表
stock_list_path = os.path.join(save_stock_list_dir,'stock_list.csv')
if not os.path.exists(stock_list_path):
    stock_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    # 将股票列表数据保存到 CSV 文件
    stock_list.to_csv(stock_list_path, index=False)
    print('Stock list saved successfully.')
else:
    stock_list = pd.read_csv(stock_list_path)

# returns: 
# ts_code	str	Y	TS代码
# symbol	str	Y	股票代码
# name	str	Y	股票名称
# area	str	Y	地域
# industry	str	Y	所属行业
# list_date	str	Y	上市日期
# e.g.
# ts_code     symbol     name     area industry    list_date
# 0     000001.SZ  000001  平安银行   深圳       银行  19910403
# 1     000002.SZ  000002   万科A   深圳     全国地产  19910129

# 获取每只股票的后复权数据并保存到文件
count = 0
for index, row in tqdm(stock_list.iterrows()):
    ts_code = row['ts_code']
    name = row['name']
    # ts_code = '002714.SZ'
    # ts_name = '牧原股份'
    # ts_code = '601127.SH'
    # ts_name = '赛力斯'

    # df = pro.daily(ts_code='000001.SZ', start_date=start_date, end_date=end_date)
    # - ts_code: 股票代码 
    # - trade_date: 交易日期 -> date
    # - open: 开盘价
    # - high: 最高价
    # - low: 最低价
    # - close: 收盘价
    # - pre_close: 昨收价(前复权), 当日收盘价 × 当日复权因子 / 最新复权因子
    # - change: 涨跌额
    # - pct_chg: 涨跌幅（未复权，如果是复权请使用通用行情接口）
    # - vol: 成交量（手）-> volume
    # - amount: 成交额（千元）

    filename = os.path.join(save_data_dir, f'{ts_code}_{name}.csv')
    if os.path.exists(filename):
        continue

    hfq = ts.pro_bar(ts_code=ts_code, adj='hfq', start_date=start_date, end_date=end_date, adjfactor=True)
    # 后复权	当日收盘价 × 当日复权因子，已check
    # qlib默认使用后复权数据，带factor，以便于计算回raw price
    
    # 将数据向qlib的字段转换
    df = hfq.rename(columns={'trade_date':'date', 'vol':'volume', 'adj_factor':'factor'})
    # 注意这里删除了amount，使用了raw volume，而不是adjusted volume；目前不清楚adjusted volume如何获取；
    df = df.drop(['ts_code','change', 'pct_chg','pre_close','amount'], axis=1)
    # name after the stock is ok, otherwise 
    # df['symbol'] = f'{ts_code}_{name}'

    # 将数据保存到文件
    df.to_csv(filename, index=False)
    print(f'{filename} saved successfully.')

    # pro_bar接口每分钟限制为200次
    count += 1
    if count % 200 == 0:
        time.sleep(63) #大于60秒 