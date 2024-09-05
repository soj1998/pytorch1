import pandas as pd
import pymysql
from sqlalchemy import create_engine


conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='abc', charset="utf8")
engine = create_engine('mysql+pymysql://root:123456@localhost/abc?charset=utf8')
df = pd.DataFrame(data=[['2']], columns=['name'])
df.to_sql('aa', engine, if_exists='append', index=False)