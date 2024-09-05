import os
import pandas as pd
import akshare as ak
import warnings
from datetime import datetime
from multiprocessing import Pool
from sqlalchemy import create_engine

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)  # 当列太多时 True就是可以换行显示。设置成False的时候不允许换行
pd.set_option('display.unicode.east_asian_width', True)

# 输入参数
start_date = '20170101'
end_date = '20240731'
adj = 'hfq'  # 复权类型：None 未复权 qfq 前复权 hfq后复权
period = 'daily'  # 周期可选：'daily,'weekly','monthly'
timeout = 10  # request超时时间

# 利用东财实时行情数据接口获取所有股票代码
df = ak.stock_zh_a_spot_em()
code_list = df[['代码', '名称']].values


# 创建文件存储路径
def create_path(ak_code):
    date_str = str(pd.to_datetime(end_date).date())  # 日期转换成字符串
    path = os.path.join('.', 'all_stock_candle', 'stock', date_str)
    # 保存数据
    if not os.path.exists(path):
        os.makedirs(path)
    file_name = ak_code + '.csv'
    return os.path.join(path, file_name)


# 获取所有股票的历史数据
def do_load(ak_code, ak_name, period, start_date, end_date, adj, timeout):
    print(ak_code, ak_name)
    for i in range(5):
        try:
            # 利用东财历史行情数据接口获取股票数据
            df = ak.stock_zh_a_hist(symbol=ak_code, period=period, start_date=start_date, end_date=end_date,
                                    adjust=adj, timeout=timeout)
            if df.empty:
                continue
            if ak_code.startswith('6'):
                df['交易所'] = '上海'
            elif ak_code.startswith('8') or ak_code.startswith('4'):
                df['交易所'] = '北京'
            elif ak_code.startswith('300'):
                df['交易所'] = '创业'
            else:
                df['交易所'] = '深圳'
            df['股票名称'] = ak_name
            df.rename(columns={'日期': '交易日期', '开盘': '开盘价', '最高': '最高价', '最低': '最低价',
                               '收盘': '收盘价'}, inplace=True)
            df = df[['交易日期', '交易所', '股票代码', '股票名称', '开盘价', '收盘价', '最高价', '最低价',
                     '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]
            df.sort_values(by=['交易日期'], ascending=True, inplace=True)  # inplace是否在原DataFrame上改动，默认为False
            df.reset_index(drop=True, inplace=True)
            path = create_path(ak_code)
            df.to_csv(path, index=False, mode='w', encoding='gbk')
            break
        except Exception as e:
            print(e)


def pd2sql(dataframe, target_t):
    engine = create_engine('mysql+pymysql://root:123456@localhost/abc?charset=utf8')
    dataframe.to_sql(target_t, engine, if_exists='append', index=False)


def do_load_df2sql(ak_code, ak_name, start_date, end_date,  timeout):
    # 复权表 3个 None 未复权 qfq 前复权 hfq后复权
    # 周期数 3个 周期可选：'daily','weekly','monthly'
    fqlx = ['', 'qfq', 'hfq']
    periodlx = ['daily', 'weekly', 'monthly']
    for i in range(len(fqlx)):
        for j in range(len(periodlx)):
            try:
                # 利用东财历史行情数据接口获取股票数据
                if i == 0:
                    df = ak.stock_zh_a_hist(symbol=ak_code, period=periodlx[j], start_date=start_date, end_date=end_date,
                                            timeout=timeout)
                else:
                    df = ak.stock_zh_a_hist(symbol=ak_code, period=periodlx[j], start_date=start_date, end_date=end_date,
                                            adjust=fqlx[i], timeout=timeout)
                if df.empty:
                    continue
                if ak_code.startswith('6'):
                    df['交易所'] = '上海'
                elif ak_code.startswith('8') or ak_code.startswith('4'):
                    df['交易所'] = '北京'
                elif ak_code.startswith('300'):
                    df['交易所'] = '创业'
                else:
                    df['交易所'] = '深圳'
                df['股票名称'] = ak_name
                df = df[['日期', '交易所', '股票代码', '股票名称', '开盘', '收盘', '最高', '最低',
                         '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]
                df.rename(columns={'日期': 'jyrq', '交易所': 'jys', '股票代码': 'gpdm', '股票名称': 'gpmc',
                                   '开盘': 'kp', '收盘': 'sp', '最高': 'zg', '最低': 'zd',
                                   '成交量': 'cyl', '成交额': 'cje', '振幅': 'zf', '涨跌幅': 'zdf', '涨跌额': 'zde',
                                   '换手率': 'hsl'}, inplace=True)
                df.sort_values(by=['交易日期'], ascending=True, inplace=True)  # inplace是否在原DataFrame上改动，默认为False
                df.reset_index(drop=True, inplace=True)
                pd2sql(df, str(fqlx[i]).lower() + str(periodlx[j]) + 'gp')
            except Exception as e:
                print(e)


if __name__ == '__main__':
    start_time = datetime.now()
    pool = Pool(8)
    pool.starmap(do_load_df2sql, [(code_list[i][0], code_list[i][1], period, start_date, end_date,
                            adj, timeout) for i in range(len(code_list))])
    pool.close()
    pool.join()
    print('获取数据时间：', datetime.now() - start_time)
