import akshare as ak
import datetime


import akshare as ak

# 000001 上证指数 399001 深证成指 399006 创业板指数
index_zh_a_hist_df = ak.index_zh_a_hist(symbol="899050", period="daily", start_date="20240901", end_date="20240905")
print(index_zh_a_hist_df)

