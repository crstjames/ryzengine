import os
import tweepy as tw
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import config as cfg
from yahoo_fin import stock_info as si
import nltk
from nltk.corpus import stopwords
import re
import networkx
from textblob import TextBlob
import sqlite3
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext
import numpy as np
from tabulate import tabulate
import requests
import math


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "ryze.db")

tgupdater = Updater(cfg.telegramAPItoken)

print(db_path)
conn = sqlite3.connect(db_path)
cursor1 = conn.cursor()
cursor2 = conn.cursor()
cursor3 = conn.cursor()

#Authenticate to Twitter
auth = tw.OAuthHandler(cfg.twitterAPIKeys["consumer_key"], cfg.twitterAPIKeys["consumer_secret"])
auth.set_access_token(cfg.twitterAPIKeys["access_token"], cfg.twitterAPIKeys["access_token_secret"])

#connect using Tweepy
api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

def analyze_twitter():
    for row in cursor1.execute('SELECT ticker_symbol, ticker_search_name, ticker_close_price, max_tweet_id FROM tblWatch ORDER BY ticker_symbol'):
        #Call out Ticker
       
        iTicker = row[0]
        search_query = row[1]
        prev_close = row[2]
        max_tweet_id = row[3]

        #Get Stock Price
        price = (si.get_live_price(iTicker))

        #print headers
        print("------------------------------")
        print("Searching on: " + search_query)

        if (max_tweet_id is None):
            tweets = tw.Cursor(api.search, 
                            q=search_query,
                            result_type="recent",
                            lang="en").items(cfg.max_results)
        else:
             tweets = tw.Cursor(api.search, 
                            q=search_query,
                            result_type="recent",
                            since_id=max_tweet_id,
                            lang="en").items(cfg.max_results)           
                
        data = [[
                tweet.id_str,
                tweet.created_at,
                iTicker,
                tweet.user.screen_name, 
                1,
                tweet.user.verified, 
                tweet.user.followers_count, 
                tweet.favorite_count, 
                textblob(remove_url(tweet.text)).sentiment.polarity,
                len(re.findall(u"\U0001F680", tweet.text)),
                #(len(re.findall(u"\U0001F680", tweet.text))/100+1) * (tweet.user.verified / 0.5 + 1) * (tweet.user.followers_count * (tweet.favorite_count/100+1) + 1)
                (tweet.user.followers_count * (tweet.favorite_count/100+1)) + tweet.user.verified/10 * (tweet.user.followers_count * (tweet.favorite_count/100+1))
                ] 
                for tweet in tweets]
                #tweet.text

        df = pd.DataFrame(data,columns=['tweet_id',
                                        'created_at',
                                        'ticker_symbol', 
                                        'screen_name',
                                        'count',
                                        'verified', 
                                        'followers', 
                                        'favorites', 
                                        'polarity',
                                        'rockets',
                                        'score'])


        pd.set_option("max_columns", None) # show all cols
        pd.set_option('max_colwidth', None) # show full width of showing cols
        pd.set_option("expand_frame_repr", False)
        pd.set_option('display.max_rows', None)
        df.set_index('tweet_id')

        if df.shape[0] > 0:
            print(str(df.shape[0]) + " Tweets to Found to Update for Ticker: " + iTicker)
            df.to_sql(name="tblHistory", con=conn, if_exists='append', index=False)
            #print('UPDATE tblWatch SET max_tweet_id = "'+ df["tweet_id"].max() + '" WHERE ticker_symbol = "'+ iTicker +'"')
            cursor2.executescript('UPDATE tblWatch SET max_tweet_id = "'+ df["tweet_id"].max() + '" WHERE ticker_symbol = "'+ iTicker +'"')
        else:
            print("No Tweets to Found to Update for Ticker: " + iTicker)

def remove_url(txt):
    """Replace URLs found in a text string with nothing 
    (i.e. it will remove the URL from the string).

    Parameters
    ----------
    txt : string
        A text string that you want to parse and remove urls.

    Returns
    -------
    The same txt string with url's removed.
    """

    return " ".join(re.sub("([^0-9A-Za-z \t])|(\w+:\/\/\S+)", "", txt).split())

def paper_trade(type,ticker):
    #print(si.get_live_price(ticker))
    print 
    if (type == "buy"):
        print(str(datetime.datetime.now())+ ": I'm buying " + math.floor((100/si.get_live_price(ticker)) + " shares at $" + si.get_live_price(ticker)))
        resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + str(datetime.datetime.now())+ ": I'm buying " + str(math.floor((100/si.get_live_price(ticker))) + " shares at $" + str(si.get_live_price(ticker))))
        
        # insert developer detail

        cursor2.executescript('INSERT INTO tblTransaction (ticker,buy_timestamp,buy_price,buy_shares VALUES ('+ ticker +',' + str(datetime.datetime.now()) + ',' + str(si.get_live_price(ticker)) + ',' + str(math.floor((100/si.get_live_price(ticker)))))
    else:
        for row in cursor1.execute('SELECT * FROM tblTransaction WHERE ticker = "' + ticker + '" and sell_price is null'):   
            print(str(datetime.datetime.now())+ ": I'm selling " + str(row[5]) + " shares at $" + si.get_live_price(ticker))
            resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + str(datetime.datetime.now())+ ": I'm selling " + str(row[5]) + " shares at $" + str(si.get_live_price(ticker)))
            cursor2.executescript('INSERT INTO tblTransaction (sell_timestamp,sell_price,sell_shares VALUES (' + str(datetime.datetime.now()) + ',' + str(si.get_live_price(ticker)) + ',' + str(row[5]))
    
    print("hello")


def analyze_history():
    #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + "." + '')
    resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + "Script run: " + str(datetime.datetime.now()) + '')
    for row in cursor1.execute('SELECT ticker_symbol, ticker_search_name, ticker_close_price, max_tweet_id FROM tblWatch ORDER BY ticker_symbol'):
        print(row[0])
        df = pd.read_sql_query('SELECT * from tblHistory WHERE ticker_symbol ="'+ row[0]+'"', conn)
        df = df.set_index('created_at')
        df.index = pd.to_datetime(df.index)
        df = df.resample('10min').agg({'count':'sum','verified':'mean','followers':'mean','favorites':'mean','polarity':'mean','rockets':'mean','score':'mean'})

        cursor3.executescript('UPDATE tblWatch SET '
                        'cur_tweets = '     + str(df.iloc[-1, 0]) + ','
                        'cur_verified = '   + str(df.iloc[-1, 1]) + ','
                        'cur_followers = '  + str(df.iloc[-1, 2]) + ','
                        'cur_favorites = '  + str(df.iloc[-1, 3]) + ','
                        'cur_polarity = '   + str(df.iloc[-1, 4]) + ','
                        'cur_rockets = '    + str(df.iloc[-1, 5]) + ','
                        'cur_score = '      + str(df.iloc[-1, 6]) + ' '
                        'WHERE ticker_symbol ="'+ row[0]+'"')

        cursor3.executescript('UPDATE tblWatch SET '
                'avg_tweets = '     + str(df["count"].mean()) + ','
                'avg_verified = '   + str(df["verified"].mean()) + ','
                'avg_followers = '  + str(df["followers"].mean()) + ','
                'avg_favorites = '  + str(df["favorites"].mean()) + ','
                'avg_polarity = '   + str(df["polarity"].mean()) + ','
                'avg_rockets = '    + str(df["rockets"].mean()) + ','
                'avg_score = '      + str(df["score"].mean()) + ' '
                'WHERE ticker_symbol ="'+ row[0]+'"')

        #print("DF1")
        #print(df)
        
        result = []
        result2 = []
        current_price = si.get_live_price(row[0])
        stock_percent = round((((si.get_live_price(row[0])) / row[2])-1)*100,4) if row[2] else 0
        stock_array =   ["Stock Price", row[2], si.get_live_price(row[0]), stock_percent]
        tweets_percent = round(((df.iloc[-1, 0] / df["count"].mean() -1 )*100),2) if df["count"].mean() else 0
        tweets_array =   ["Tweets", round(df["count"].mean(),2) , round(df.iloc[-1, 0],2), tweets_percent]
        verified_percent = round(((df.iloc[-1, 1] / df["count"].mean() -1 )*100),2) if df["verified"].mean() else 0
        verified_array =   ["Verified", round(df["verified"].mean(),2) , round(df.iloc[-1, 1],2), verified_percent]
        followers_percent = round(((df.iloc[-1, 2] / df["followers"].mean() -1 )*100),2) if df["followers"].mean() else 0
        followers_array =   ["Followers", round(df["followers"].mean(),2) , round(df.iloc[-1, 2],2), followers_percent]
        favorites_percent = round(((df.iloc[-1, 3] / df["favorites"].mean() -1 )*100),2) if df["favorites"].mean() else 0
        favorites_array =   ["Favorites", round(df["favorites"].mean(),2) , round(df.iloc[-1, 3],2),favorites_percent]
        polarity_percent = round(((df.iloc[-1, 4] / df["polarity"].mean() -1 )*100),2) if df["polarity"].mean() else 0
        polarity_array =   ["Polarity", round(df["polarity"].mean(),2) , round(df.iloc[-1, 4],2),polarity_percent]
        rockets_percent = round(((df.iloc[-1, 5] / df["rockets"].mean() -1 )*100),2) if df["rockets"].mean() else 0
        rockets_array =   ["Rockets", round(df["rockets"].mean(),2) , round(df.iloc[-1, 5],2), rockets_percent]
        score_percent = round(((df.iloc[-1, 6] / df["score"].mean() -1 )*100),2) if df["score"].mean() else 0
        score_array =   ["Score", round(df["score"].mean(),2) , round(df.iloc[-1, 6],2), score_percent]
        
        #result.append(stock_array)
        result2.append(stock_array)
        result.append(tweets_array)
        result.append(verified_array)
        result.append(followers_array)
        result.append(favorites_array)
        result.append(polarity_array)
        result.append(rockets_array)
        result.append(score_array)

        #print(tabulate(result, headers=['Stat', 'Average', 'Current', 'Percent Change']))
        df4 = pd.read_sql_query('SELECT buy_shares FROM tblTransaction WHERE ticker = "' + row[0] + '" and sell_price is null', conn)
        print(df4)
        print(score_percent)
        print(polarity_percent)
        if (score_percent > 200 and polarity_percent >= -.01 ):
            resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+  row[0] + '')
            resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+ tabulate(result, headers=['Stat', 'Average', 'Current', 'Percent']))
            if df4.shape[0] == 0:
                print(str(datetime.datetime.now())+ ": I'm buying " + str(math.floor((1000/si.get_live_price(row[0])))) + " shares at $" + str(si.get_live_price(row[0])))
                resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + str(datetime.datetime.now()) + ': Im buying ' + str(math.floor((1000/si.get_live_price(row[0])))) + " " + row[0] + " shares at $" + str(si.get_live_price(row[0])))
                sqlite_insert_with_param = """INSERT INTO 'tblTransaction'
                            ('ticker', 'buy_timestamp', 'buy_price', 'buy_shares') 
                            VALUES (?, ?, ?, ?);"""
                data_tuple = (row[0], datetime.datetime.now(), si.get_live_price(row[0]), math.floor(1000/si.get_live_price(row[0])))    
    
                cursor2.execute(sqlite_insert_with_param,data_tuple)
                print(sqlite_insert_with_param,data_tuple)
                cursor2.close()
        else:
            if df4.shape[0] > 0:
                print(df4['buy_shares'])
                #print(str(datetime.datetime.now())+ ": I'm selling " + str(df3['buy_shares']) + " shares at $" + str(si.get_live_price(row[0])))
                resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + str(datetime.datetime.now()) + ': Im selling ' + str(math.floor((1000/si.get_live_price(row[0])))) + " " + row[0] + " shares at $" + str(si.get_live_price(row[0])))
                
                sqlite_update_with_param = """UPDATE tblTransaction SET
                            sell_timestamp = ?,
                            sell_price = ?,
                            sell_shares = ?
                            WHERE ticker = ?;"""
                data_tuple = (datetime.datetime.now(), si.get_live_price(row[0]), df4.iloc[0]['buy_shares'], row[0])
                cursor2.execute(sqlite_update_with_param,data_tuple)
                cursor2.close()
            else:
                print("--")
            #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+ row[0] + ' (silence) ')
            #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+ row[0] + ' - Nothing Happening - ' + str(datetime.datetime.now()))
            #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+ tabulate(result2, headers=['Stat', 'Closed', 'Current', 'Percent Change']))
    conn.commit()

gather_twitter_data()




