import pandas as pd
import pymysql
from sqlalchemy import create_engine


engine = create_engine('mysql+pymysql://root:Abcd1234.@175.24.228.81/mygp?charset=utf8')
df = pd.DataFrame(data=[['2']], columns=['name'])
df.to_sql('aa', engine, if_exists='append', index=False)