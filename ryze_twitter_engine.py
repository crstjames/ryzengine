
import io
import logging
import os
import pandas as pd
import psycopg2
import re
from sqlalchemy import create_engine
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext
from textblob import TextBlob
import tweepy as tw
from yahoo_fin import stock_info as si




# Init Logging
logging.basicConfig(
	filename='errlog.log',
	level=logging.WARNING,
	format='%(asctime)s:%(levelname)s:%(message)s',
)
# Connect to your postgres DB
conn = psycopg2.connect(
            host="ec2-35-174-118-71.compute-1.amazonaws.com",
            database="d52b44lrc9ub0m",
            user="swpfkjtbixewxs",
            password="b4bf128f59fa7e9cb22e7d0e38574fb383505ee9b73c09f2c8d10e4189ac0a57",
            port="5432")

# Open a cursor to perform database operations
cur = conn.cursor()
tblwatch_select_query = "SELECT ticker_symbol, ticker_search_name, max_tweet_id FROM public.tblWatch ORDER BY ticker_symbol"
cur.execute(tblwatch_select_query)
#print(cur.fetchall())

engine = create_engine('postgresql://swpfkjtbixewxs:b4bf128f59fa7e9cb22e7d0e38574fb383505ee9b73c09f2c8d10e4189ac0a57@ec2-35-174-118-71.compute-1.amazonaws.com:5432/d52b44lrc9ub0m')

#Twitter API KEYS
auth = tw.OAuthHandler("KqGmXU8Bo1IdpBzq824HQVEBe", "ELJJeSwnDUScsm1UFcWjFJAal7lw2YhyyMJqUTDYCXtrn0sbaR")
auth.set_access_token("28787053-9RJW0D324Lic7ChT1l7hf52BeP4GpJoycsfNxnveT", "dGRQBOz0X3QeVZuJIu1p2w9X5A3hbcSq65OKByAxUGCly")

#Authenticate to Twitter
#auth = tw.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
#auth.set_access_token(twitter_access_token, twitter_access_token_secret)

#connect using Tweepy
api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

#Telegram Bot API Token
telegramAPItoken            = "1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY"
tgupdater = Updater(telegramAPItoken)
#Script Configurations
max_results  = 1500
cron_job = "*/10 * * * * /Library/Frameworks/Python.framework/Versions/3.8/bin/python3 /Users/xstjames/Code/Development/ryze/engine.p"

#File Path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_twitter_data():
    tblwatch_select_query = "SELECT ticker_symbol, ticker_search_name, max_tweet_id FROM public.tblWatch ORDER BY ticker_symbol"
    cur.execute(tblwatch_select_query)
    for row in cur.fetchall() :
        #Call out Ticker
       
        iTicker = row[0]
        search_query = row[1]
        max_tweet_id = row[2]

        print(iTicker,search_query,max_tweet_id)

        #Get Stock Price
        price = (si.get_live_price(iTicker))

        #print headers
        print("------------------------------")
        print("Searching on: " + search_query)

        if (max_tweet_id is None):
            tweets = tw.Cursor(api.search, 
                            q="search_query",
                            result_type="recent",
                            lang="en").items(100)
        else:
             tweets = tw.Cursor(api.search, 
                            q=search_query,
                            result_type="recent",
                            since_id=max_tweet_id,
                            lang="en").items(100)           
                
        data = [[
                tweet.id_str,
                tweet.created_at,
                iTicker,
                tweet.user.screen_name, 
                1,
                tweet.user.verified, 
                tweet.user.followers_count, 
                tweet.favorite_count, 
                TextBlob(remove_url(tweet.text)).sentiment.polarity,
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
            df.to_sql('tblhistory', con=engine, index=False, if_exists='append')
            tblwatch_update_query = "UPDATE tblWatch SET max_tweet_id = "+ df["tweet_id"].max() + " WHERE ticker_symbol = '"+ str(iTicker) +"'"
            cur.execute(tblwatch_update_query)
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


get_twitter_data()
conn.commit()