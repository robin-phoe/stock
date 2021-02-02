import tushare as ts
import pandas as pd
token = '87b1df604b58eac3662ebaeabe6bb3436792125d2bf1f73a4a11f06a'
ts.set_token(token)
pro = ts.pro_api()
df = pro.daily(ts_code='002750.SZ', start_date='20201105', end_date='20201106')
print(df)
df.to_csv('test_tushare.csv')