import requests
from datetime import datetime
import pandas as pd
from time import sleep
import sys
from tqdm import tqdm


# api info
coinbinance = sys.argv[1].upper()
coin_mb = sys.argv[2].upper()
n_obs = int(sys.argv[3])
secs = int(sys.argv[4])
method = 'ticker'
url_mb = f'https://www.mercadobitcoin.net/api/{coin_mb}/{method}/'
urlbinance = f'https://api.binance.com/api/v3/trades?symbol={coinbinance}'

dfs=[]

for i in tqdm(range(n_obs)):
    # get data
    response_mb = requests.get(url_mb)
    response_json_mb = response_mb.json()

    response_binance = requests.get(urlbinance)
    response_json_binance = response_binance.json()


    # build dataframes
    df_mb = pd.DataFrame(response_json_mb['ticker'], index=[0])
    df_mb['date'] = datetime.fromtimestamp(df_mb['date']).strftime("%d-%b-%Y-%H:%M:%S")


    df_binance = pd.DataFrame(response_json_binance[-1], index=[0])
    df_binance['time'] = datetime.fromtimestamp(df_binance['time']/1000).strftime("%d-%b-%Y-%H:%M:%S")

    df = df_mb[['date', 'last']].join(df_binance[['time','price']])
    df.rename(columns ={'date':'time_mb', 'last':'price_mb', 'time':'time_binance', 'price':'price_binance'}, inplace = True)

    dfs.append(df)
    sleep(secs)

df = dfs[0].append(dfs[1:]).reset_index(drop=True)
# print(df_mb.T)
# print(df_binance.T)
print(df)
df.to_excel(f'coin_mb_arbitrage_price.xlsx', index=False)