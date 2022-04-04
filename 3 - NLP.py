from logging import exception
from os import sep
import tweepy, sys
import pandas as pd
from openpyxl import load_workbook
from time import sleep
from pandas import concat
from langdetect import detect
from flair.models import TextClassifier
from flair.data import Sentence
from segtok.segmenter import split_single
import numpy as np
import re
import datetime as dt

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

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # the query to be searched
    elements = ['BTC ', 'Bitcoin', 'Etherum', 'ETH ', "crypto ", "cryptocurrency", "cryptocurrencies ", "altcoin ", "Uniswap ", 
                    "Polkadot", "1inch", "PancakeSwap", "SushiSwap", "Ripple ", "Algorand ", "MATIC ", "Chainlink ", "BCH ", 
                        "Cardano ", "Shibtoken ", "Dogecoin ", "OBJBTC ", "NFT ", "BNB ", "Tezos ", "USDC "]
    hashtag = ["#", "$"]
    tweets_filter = []
    for text in elements:
        for tag in hashtag :
            q = tag + text
            tweets_filter.append(q)
        tweets_filter.append(text)
    print(tweets_filter)

    # Regex to clean emoticon
    emoji_pattern = re.compile("["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                            "]+", flags=re.UNICODE)

    text = u'This dog \U0001f602'
    text = emoji_pattern.sub(r'', text)
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
    # try : 
    #     BDD = pd.read_csv(BDD_raw_file, sep=',', dtype = {'username': str,'created_at': str,
    #                                                   'text': str,'ID': str})
    #     print("BDD read OK")
    # except Exception as e: 
    #     # print(e)
    #     print("BDD CREATED")
    #     BDD = pd.DataFrame(columns=['username', 'created_at', 'text'])
    # DF Done Usernames
    try : 
        df_done_usernames = pd.read_csv(done_usernames_file, sep=',')
        print("df_done_usernames read OK")
    except Exception as e: 
        # print(e)
        print("df_done_usernames CREATED")
        df_done_usernames = pd.DataFrame(columns=['username'])


def save_to_csv(dataframe, file):
    try : 
        dataframe.to_csv(file, index = False, encoding='utf-8-sig')
        print("Dataframe saved corretly")
    except Exception as e:
        print(e)
        backup_file = f"{file[0: -4]}_backup.csv"
        dataframe.to_csv(backup_file, index = False, encoding='utf-8-sig')
        print("Dataframe BACKUP saved corretly")



    return text


def filtering(text, tweets_filter):
    text = str(text)
    text = emoji_pattern.sub(r'', text)
    text = text.replace('\n', ' ')
    text = text.replace(',', ' ')
    text = text.replace(';', ' ')        
    found = 0
    for element in tweets_filter:
        if found == 1 : return 1
        if text.find(element) > 0 : 
            found = 1
    return 0


# df1 = pd.read_csv(r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\2 - BDD raw.csv", sep=',')
# df2 = pd.read_csv(r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\2 - BDD raw suite.csv", sep=',')
# frames = [df1, df2]
# BDD = concat(frames)
# print(BDD)


# # FILTERING tweets only related to Bitcoin (any element in tweets_filter list)
# BDD["filter"] = BDD.apply(lambda row: filtering(row["text"], tweets_filter) , axis = 1)
# BDD = BDD[BDD['filter'] == 1]
# print(BDD)




# # REMOVING chinese identified tweets
# def language(text):
#     try : 
#         lang = detect(text)
#         return lang
#     except Exception as e: 
#         print(e)
#         print("Text : ", text)
#         return "undefined"

# BDD["lang"] = BDD["text"].apply(language)
# BDD = BDD[BDD["lang"] != "vi"]
# BDD = BDD[BDD["lang"] != "zh-cn"] 
# print(BDD)

# BDD = BDD.drop_duplicates(subset=['text'])
# print("Without duplicates :")
# print(BDD)


BDD = pd.read_csv(r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\3 - BDD filtered.csv", 
                    sep=',')
print("BDD read OK")
print(BDD)  




# Setting the sentiment of the tweet with NLP Analysis
classifier = TextClassifier.load('en-sentiment')
def predict(sentence):
    """ Predict the sentiment of a sentence """
    if sentence == "":
        return 0
    text = Sentence(sentence)
    # stacked_embeddings.embed(text)
    classifier.predict(text)
    value = text.labels[0].to_dict()['value'] 
    if value == 'POSITIVE':
        result = text.to_dict()['labels'][0]['confidence']
    else:
        result = -(text.to_dict()['labels'][0]['confidence'])
    return round(result, 3)
def clean(raw):
    """ Remove hyperlinks and markup """
    result = re.sub("<[a][^>]*>(.+?)</[a]>", 'Link.', raw)
    result = re.sub('&gt;', "", result)
    result = re.sub('&#x27;', "'", result)
    result = re.sub('&quot;', '"', result)
    result = re.sub('&#x2F;', ' ', result)
    result = re.sub('<p>', ' ', result)
    result = re.sub('</i>', '', result)
    result = re.sub('&#62;', '', result)
    result = re.sub('<i>', ' ', result)
    result = re.sub("\n", '', result)
    return result
def make_sentences(text):
    """ Break apart text into a list of sentences """
    sentences = [sent for sent in split_single(text)]
    return sentences
def get_scores(sentences):
    """ Call predict on every sentence of a text """
    results = []
    try : 
        for i in range(0, len(sentences)): 
            results.append(predict(sentences[i]))
        return results
    except Exception as e :
        return 0
def get_sum(scores):
    
    result = round(sum(scores), 3)
    return result

try : 
    print("Début clean")
    BDD["text"] = BDD["text"].apply(clean)
    print("Début sentence")
    BDD['sentences'] = BDD["text"].apply(make_sentences)
    print("Début scores")
    BDD['scores'] = BDD['sentences'].apply(get_scores)
    print("Début sum")
    BDD['scores_sum'] = BDD['scores'].apply(get_sum)


    # SET default SENTIMENT to 1 for the following categories : 
    pro_categories = ["Cryto Influencer", "Spécialistes crypto", "Crypto compagnies", "Cryptocurrencies"]
    BDD['Sentiment'] = np.where(BDD['category'].isin(pro_categories), 1, BDD['scores_sum'])
    BDD['Sentiment'] = np.where(BDD['Sentiment'] > 0, 1, -1)
    print(BDD)

    save_to_csv(r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\3 - BDD NLP.csv", filtered_BDD_file)

    BDD = BDD[BDD["scores_sum"] != 0] 
    print(BDD)
    save_to_csv(r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\3 - BDD NLP 2.csv", filtered_BDD_file)

except Exception as e :
    print(e)
    save_to_csv(r"C:\Users\guspi\Documents\Ecole\ESSCA\5A\Mémoire\Twitter\V2 - mémoire\3 - BDD NLP.csv", filtered_BDD_file)


