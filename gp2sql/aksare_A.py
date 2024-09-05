import akshare as ak


class Stock_AkShare():
    def __init__(self, date='20240830'):
        self.date = date
        self.dateny = self.date[:6]

    '''
    接口: stock_sse_summary
    目标地址: http://www.sse.com.cn/market/stockdata/statistic/
    描述: 上海证券交易所-股票数据总貌
    限量: 单次返回最近交易日的股票数据总貌(当前交易日的数据需要交易所收盘后统计)
    '''
    def stock_sse_summary_df(self):
        stock_sse_summary_df = ak.stock_sse_summary()
        return stock_sse_summary_df

    '''
    深圳证券交易所
    证券类别统计
    接口: stock_szse_summary
    目标地址: http://www.szse.cn/market/overview/index.html
    描述: 深圳证券交易所-市场总貌-证券类别统计
    限量: 单次返回指定 date 的市场总貌数据-证券类别统计(当前交易日的数据需要交易所收盘后统计)
    '''
    def stock_szse_summary_df(self):
        stock_szse_summary_df = ak.stock_szse_summary(self.date)
        print(stock_szse_summary_df)

    '''
    深圳证券交易所
    地区交易排序
    接口: stock_szse_area_summary
    目标地址: http://www.szse.cn/market/overview/index.html
    描述: 深圳证券交易所-市场总貌-地区交易排序
    限量: 单次返回指定 date 的市场总貌数据-地区交易排序数据
    '''
    def stock_szse_area_summary_df(self):
        stock_szse_area_summary_df = ak.stock_szse_area_summary(self.dateny)
        print(stock_szse_area_summary_df)

    '''
    深圳证券交易所
    股票行业成交
    接口: stock_szse_sector_summary
    目标地址: http://docs.static.szse.cn/www/market/periodical/month/W020220511355248518608.html
    描述: 深圳证券交易所-统计资料-股票行业成交数据
    限量: 单次返回指定 symbol 和 date 的统计资料-股票行业成交数据
    '''
    def stock_szse_sector_summary_df(self):
        stock_szse_sector_summary_df = ak.stock_szse_sector_summary(symbol="当月", date=self.dateny)
        print(stock_szse_sector_summary_df)

    '''
    上海证券交易所-每日概况
    接口: stock_sse_deal_daily
    目标地址: http://www.sse.com.cn/market/stockdata/overview/day/
    描述: 上海证券交易所-数据-股票数据-成交概况-股票成交概况-每日股票情况
    限量: 单次返回指定日期的每日概况数据, 当前交易日数据需要在收盘后获取; 注意在 20211227（包含）之后输出格式变化       
    '''

    def stock_sse_deal_daily_df(self):
        stock_sse_deal_daily_df = ak.stock_sse_deal_daily(date=self.date)
        print(stock_sse_deal_daily_df)




akshare = Stock_AkShare(date='20210730')
akshare.stock_sse_deal_daily_df()
akshare = Stock_AkShare(date='20240730')
akshare.stock_sse_deal_daily_df()


# 000001 上证指数 399001 深证成指 399006 创业板指数 899050 北证50
index_zh_a_hist_df = ak.index_zh_a_hist(symbol="899050", period="daily", start_date="20240901", end_date="20240905")
print(index_zh_a_hist_df)