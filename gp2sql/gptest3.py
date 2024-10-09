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
start_date = '20241008'
end_date = '20241008'
adj = 'hfq'  # 复权类型：None 未复权 qfq 前复权 hfq后复权
period = 'daily'  # 周期可选：'daily,'weekly','monthly'
timeout = 100  # request超时时间

# 利用东财实时行情数据接口获取所有股票代码
df = ak.stock_zh_a_spot_em()
code_list = df[['代码', '名称']].values
# 复权表 3个 None 未复权 qfq 前复权 hfq后复权
# 周期数 3个 周期可选：'daily','weekly','monthly'
fqlx = ['', 'qfq', 'hfq']
periodlx = ['daily', 'weekly', 'monthly']


def pd2sql(dataframe, target_t):
    engine = create_engine('mysql+pymysql://root:Abcd1234.@175.24.228.81/mygp?charset=utf8')
    a = dataframe['gpmc']
    print(a.values[0])
    dataframe.to_sql(target_t, engine, if_exists='append', index=False)


def do_load_df2sql_gp(ak_code, ak_name, start_date, end_date, timeout):
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
                df['类型'] = '股票'
                df['复权'] = fqlx[i]
                df['日期类型'] = periodlx[j]
                df['成交额'] = df['成交额'].map(lambda x: x / 10000)
                df = df[['类型', '日期', '交易所', '股票代码', '股票名称', '复权',
                         '日期类型', '开盘', '收盘', '最高', '最低',
                         '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]
                df.rename(columns={'类型': 'fenlei', '日期': 'jyrq', '交易所': 'jys', '股票代码': 'gpdm', '股票名称': 'gpmc',
                                   '复权': 'fq', '日期类型': 'period',
                                   '开盘': 'kp', '收盘': 'sp', '最高': 'zg', '最低': 'zd',
                                   '成交量': 'cyl', '成交额': 'cje', '振幅': 'zf', '涨跌幅': 'zdf', '涨跌额': 'zde',
                                   '换手率': 'hsl'}, inplace=True)
                df.sort_values(by=['jyrq'], ascending=True, inplace=True)  # inplace是否在原DataFrame上改动，默认为False
                df.reset_index(drop=True, inplace=True)
                pd2sql(df, 'qbgp')
            except Exception as e:
                pd2sql(df, 'wbcgp')
                print(e)


def do_load_df2sql_index(ak_code, ak_name, start_date, end_date):
    for j in range(len(periodlx)):
        try:
            # 利用东财历史行情数据接口获取股票数据
            df = ak.index_zh_a_hist(symbol=ak_code, period=periodlx[j], start_date=start_date, end_date=end_date)
            if df.empty:
                continue
            if ak_code.startswith('000001'):
                df['交易所'] = '上海'
            elif ak_code.startswith('399006'):
                df['交易所'] = '创业'
            else:
                df['交易所'] = '深圳'
            df['股票代码'] = ak_code
            df['股票名称'] = ak_name
            df['类型'] = '指数'
            df['复权'] = ''
            df['日期类型'] = periodlx[j]
            df['成交额'] = df['成交额'].map(lambda x: x / 100000000)
            df = df[['类型', '日期', '交易所', '股票代码', '股票名称', '复权',
                     '日期类型', '开盘', '收盘', '最高', '最低',
                     '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]
            df.rename(
                columns={'类型': 'fenlei', '日期': 'jyrq', '交易所': 'jys', '股票代码': 'gpdm', '股票名称': 'gpmc',
                         '复权': 'fq', '日期类型': 'period',
                         '开盘': 'kp', '收盘': 'sp', '最高': 'zg', '最低': 'zd',
                         '成交量': 'cyl', '成交额': 'cje', '振幅': 'zf', '涨跌幅': 'zdf', '涨跌额': 'zde',
                         '换手率': 'hsl'}, inplace=True)
            df.sort_values(by=['jyrq'], ascending=True, inplace=True)  # inplace是否在原DataFrame上改动，默认为False
            df.reset_index(drop=True, inplace=True)
            pd2sql(df, 'qbgp')
        except Exception as e:
            print(e)


if __name__ == '__main__':
    start_time = datetime.now()
    index_list = [['000001', '上证指数'], ['399001', '深证成指'], ['399006', '创业板指数']]
    pool1 = Pool(3)
    pool1.starmap(do_load_df2sql_index, [(index_list[i][0], index_list[i][1], start_date,
                                          end_date) for i in range(len(index_list))])
    pool1.close()
    pool1.join()
    print('指数搞完', datetime.now() - start_time)
    start_time = datetime.now()
    pool = Pool(3)
    pool.starmap(do_load_df2sql_gp, [(code_list[i][0], code_list[i][1], start_date, end_date,
                             timeout) for i in range(len(code_list))])
    pool.close()
    pool.join()

    # for i in range(len(index_list)):
    #     do_load_df2sql_index(index_list[i][0], index_list[i][1], start_date, end_date)
    # for i in range(len(code_list)):
    #     do_load_df2sql_gp(code_list[i][0], code_list[i][1], start_date, end_date, timeout)
    print('获取数据时间：', datetime.now() - start_time)
