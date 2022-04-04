from copy import copy
from operator import index
from os import sep
import tweepy, sys
import pandas as pd
from time import sleep
from pandas import concat
import numpy as np
import datetime as dt
from random import randrange

consumer_key = "CLfG4dmcSLqGTO7fRjI1TVakE"
consumer_secret = "cXqMVZ21oviFhl6FmaeKdty3QJW9tmsf9nPzgOSAbT3Xofiiqs"
access_token = "1089879351276040194-l4BF0ffyBLh5qUKbVy7eXpRjhJ2GNF"
access_token_secret = "c1xIOG3YYmdWHoEbhNwZ6xlTxuVVhTNcxVN7tu89hsY82"

names_file = r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\1 - names_file.csv"
not_found_names_file = r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\1 - not_found_names_file.csv"
usernames_file = r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\1 - usernames_file.csv"
BDD_raw_file = r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\2 - BDD raw - Copie (2).csv"
done_usernames_file = r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\2 - done_usernames_file.csv"
prices_file = r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\4 - full_prices.csv"
result_file = r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\4 - result.csv"
filtered_BDD_file = r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\2 - BDD filtered + NLP.csv"


# Make list with #, $ and nothing for each element
for i in range(1):

    #Dataframes
    # DF names
    try : 
        df_names = pd.read_csv(names_file, sep=',')
        print("df_names read OK")
    except Exception as e: 
        # print(e)
        sys.exit()
    # DF not found names
    try : 
        df_not_found = pd.read_csv(not_found_names_file, sep=',')
        print("not_found_names read OK")
    except Exception as e: 
        # print(e)
        print("not_found_names CREATED")
        df_not_found = pd.DataFrame(columns=['Name'])
    # DF usernames found
    try : 
        df_usernames = pd.read_csv(usernames_file, sep=',')
        print("df_usernames read OK")
    except Exception as e: 
        # print(e)
        print("df_usernames CREATED")
        df_usernames = pd.DataFrame(columns=['Name', 'username'])
    # DF BDD
    try : 
        BDD = pd.read_csv(filtered_BDD_file, sep=',', dtype = {'username': str,'created_at': str,
                                                      'text': str,'ID': str, 'Sentiment':float})
        print("BDD read OK")
    except Exception as e: 
        # print(e)
        print("BDD CREATED")
        BDD = pd.DataFrame(columns=['username', 'created_at', 'text'])
    # DF Done Usernames
    try : 
        df_done_usernames = pd.read_csv(done_usernames_file, sep=',')
        print("df_done_usernames read OK")
    except Exception as e: 
        # print(e)
        print("df_done_usernames CREATED")
        df_done_usernames = pd.DataFrame(columns=['username'])
    # DF prices
    try : 
        df_prices = pd.read_csv(prices_file, sep=',')
        print("df_prices read OK")
    except Exception as e: 
        print(e)
        sys.exit()


def save_to_csv(dataframe, file):
    try : 
        dataframe.to_csv(file, index = False, encoding='utf-8-sig')
        print("Dataframe saved corretly")
    except Exception as e:
        print(e)
        backup_file = f"{file[0: -4]}_backup.csv"
        dataframe.to_csv(backup_file, index = False, encoding='utf-8-sig')







df_prices['Time'] = pd.to_datetime(df_prices['Time'])
df_prices = df_prices.sort_values(by='Time')
BDD['created_at'] = pd.to_datetime(BDD['created_at'])
BDD = BDD.dropna()
BDD = BDD.sort_values(by='created_at')
print(BDD)
BDD['24h rdm (%)'] = 0
BDD['24h best rdm (%)'] = 0
BDD = BDD.reset_index(drop=True)
print(BDD)


for index, row in BDD.iterrows():
    
    print(row['username'], row['text'])
    
    # df = df_prices.copy()
    # tweet_date = row["created_at"]

    # end = tweet_date + dt.timedelta(hours=6)
    # mask = (df['Time'] >= tweet_date) & (df['Time'] <= end)
    # df = df.loc[mask]
    # if df.empty == True : row["24h rdm (%)"] = np.nan ; row['24h best rdm (%)'] = np.nan ; continue

    # rendement = np.round(((df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0] ) * 100, decimals=2)
    # row["24h rdm (%)"] = rendement

    # prices = df['Close'].tolist()
    # cummin = np.minimum.accumulate
    # max_return = np.max((prices - cummin(prices) ) / cummin(prices)  * 100 )
    # max_return = np.round(max_return, decimals=2)
    # row['24h best rdm (%)'] = max_return

sys.exit()

for x in range(len(BDD)):
    df = df_prices.copy()
    tweet_date = BDD.iloc[x, BDD.columns.get_loc("created_at")]

    end = tweet_date + dt.timedelta(hours=6)
    mask = (df['Time'] >= tweet_date) & (df['Time'] <= end)
    df = df.loc[mask]
    if df.empty == True : BDD["24h rdm (%)"].iloc[x] = np.nan ; BDD['24h best rdm (%)'].iloc[x] = np.nan ; continue

    rendement = np.round(((df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0] ) * 100, decimals=2)
    BDD["24h rdm (%)"].iloc[x] = rendement

    prices = df['Close'].tolist()
    cummin = np.minimum.accumulate
    max_return = np.max((prices - cummin(prices) ) / cummin(prices)  * 100 )
    max_return = np.round(max_return, decimals=2)
    BDD['24h best rdm (%)'].iloc[x] = max_return

BDD = BDD.dropna()
BDD = BDD.reset_index(drop=True)
print(BDD)
BDD['Correlated'] = np.where(((BDD['Sentiment'] >= 0) & (BDD['24h rdm (%)'] >= 0))  |  ((BDD['Sentiment'] < 0) & (BDD['24h rdm (%)'] < 0))   , 1, 0)
print(BDD)



df_final_usernames = pd.DataFrame()


unique_usernames = BDD['username'].unique()

for username in unique_usernames:
    row = {}

    copy_BDD = BDD[BDD['username'] == username] 

    total_tweets = len(copy_BDD)
    correlated = copy_BDD['Correlated'].sum()
    mean_impact = copy_BDD['24h best rdm (%)'].mean()


    row.update({'username' : username, 'total tweet': int(total_tweets), 'correlated tweets' : int(correlated), 'mean impact' : mean_impact}) 
    df_final_usernames = df_final_usernames.append(row, ignore_index = True)


df_final_usernames['mean impact'] = np.round(df_final_usernames['mean impact'], decimals= 2)
# df_final_usernames = df_final_usernames.set_index('username')
df_final_usernames['correlated tweets (%)'] = np.round((df_final_usernames['correlated tweets'] / df_final_usernames['total tweet'] ) * 100, decimals=2)
df_final_usernames = df_final_usernames.sort_values(by ='correlated tweets (%)', ascending=False)
print(df_final_usernames)
save_to_csv(df_final_usernames, result_file)
sys.exit()


#Final filter : remove coïncidences
df_final_usernames = df_final_usernames[df_final_usernames['correlated tweets'] > 0,8]



# Results :

df_results = pd.read_csv(result_file, sep=',')
print(df_results)

df_results = df_results[df_results['correlated tweets (%)'] > 80]
df_results = df_results[df_results['correlated tweets'] > 1]
df_results = df_results.reset_index(drop=True)
print(df_results)

sys.exit()