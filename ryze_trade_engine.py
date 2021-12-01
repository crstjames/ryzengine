
import io
import logging
import os
import pandas as pd
import psycopg2
import re
from sqlalchemy import create_engine
from textblob import TextBlob
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
engine = create_engine('postgresql://swpfkjtbixewxs:b4bf128f59fa7e9cb22e7d0e38574fb383505ee9b73c09f2c8d10e4189ac0a57@ec2-35-174-118-71.compute-1.amazonaws.com:5432/d52b44lrc9ub0m')

#Telegram Bot API Token
telegramAPItoken            = "1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY"

def analyze_history():
    #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + "." + '')
    #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + "Script run: " + str(datetime.datetime.now()) + '')
    tblwatch_select_query = "SELECT ticker_symbol, ticker_search_name, ticker_close_price, max_tweet_id FROM tblWatch ORDER BY ticker_symbol"
    cur.execute(tblwatch_select_query)
    for row in cur.fetchall() :
        print(row[0])
        df = pd.read_sql_query('SELECT * from tblHistory WHERE ticker_symbol ="'+ row[0]+'"', conn)
        df = df.set_index('created_at')
        df.index = pd.to_datetime(df.index)
       
        df2 = df
        df2 = df2.rolling('10m', min_periods=1).agg({'count':'sum','verified':'mean','followers':'mean','favorites':'mean','polarity':'mean','rockets':'mean','score':'mean'})

        df = df.resample('10min').agg({'count':'sum','verified':'mean','followers':'mean','favorites':'mean','polarity':'mean','rockets':'mean','score':'mean'})
        
        tblwatch_update_query = 'UPDATE tblWatch SET '
                        'cur_tweets = '     + str(df.iloc[-1, 0]) + ','
                        'cur_verified = '   + str(df.iloc[-1, 1]) + ','
                        'cur_followers = '  + str(df.iloc[-1, 2]) + ','
                        'cur_favorites = '  + str(df.iloc[-1, 3]) + ','
                        'cur_polarity = '   + str(df.iloc[-1, 4]) + ','
                        'cur_rockets = '    + str(df.iloc[-1, 5]) + ','
                        'cur_score = '      + str(df.iloc[-1, 6]) + ' '
                        'WHERE ticker_symbol ="'+ row[0]+'"'
        cur.execute(tblwatch_update_query)

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
        df4 = pd.read_sql_query('SELECT buy_shares,buy_price FROM tblTransaction WHERE ticker = "' + row[0] + '" and sell_price is null', conn)
        #print(df4)

        print('Score: ' + str(score_percent) + '% Polarity: ' + str(polarity_percent))
        if (score_percent > 200 and polarity_percent > -.01):
            resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+  row[0] + '')
            #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+ tabulate(result, headers=['Stat', 'Average', 'Current', 'Percent']))
            if df4.shape[0] == 0:
                print(str(datetime.datetime.now())+ ": I'm buying " + str(math.floor((1000/si.get_live_price(row[0])))) + " shares at $" + str(si.get_live_price(row[0])))
                
                resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text=' + str(datetime.datetime.now()) + ': Im buying ' + str(math.floor((1000/si.get_live_price(row[0])))) + " " + row[0] + " shares at $" + str(si.get_live_price(row[0])))
                
                sqlite_insert_with_param = """INSERT INTO 'tblTransaction'
                            ('ticker', 'buy_timestamp', 'buy_price', 'buy_shares') 
                            VALUES (?, ?, ?, ?);"""
                data_tuple = (row[0], datetime.datetime.now(), si.get_live_price(row[0]), math.floor(1000/si.get_live_price(row[0])))    

                cursor6.execute(sqlite_insert_with_param,data_tuple)
                print(sqlite_insert_with_param,data_tuple)
                

                currPrice =  si.get_live_price(row[0])
                stopLoss =   str(currPrice - (currPrice *.1))
                takeProfit = str(currPrice + (currPrice *.2))
                quantity =   math.floor((1000/currPrice))
        
                order = alpacaTradeAPI.submit_order(str(row[0]), 
                    qty=quantity, 
                    side='buy', 
                    time_in_force='day', 
                    type='market',
                    order_class='bracket', 
		            stop_loss=dict(stop_price=stopLoss), 
		            take_profit=dict(limit_price=takeProfit))
                print(order)

            else:
                resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+  row[0] + ' We holdin baby!')
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
                data_tuple = (datetime.datetime.now(), si.get_live_price(row[0]), str(df4.iloc[0]['buy_shares']), row[0])
                cursor7.execute(sqlite_update_with_param,data_tuple)
                
                sellQuantity = str(df4.iloc[0]['buy_shares'])
                limitPrice = str(df4.iloc[0]['buy_price'])
                
                order = alpacaTradeAPI.submit_order(symbol=str(row[0]), 
                            qty=sellQuantity, 
                            side='sell', 
                            time_in_force='day', 
                            type='limit',
                            limit_price=limitPrice)
                print(order)
                print(sqlite_update_with_param,data_tuple)
            else:
                print("--")
            #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+ row[0] + ' (silence) ')
            #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+ row[0] + ' - Nothing Happening - ' + str(datetime.datetime.now()))
            #resp = requests.post('https://api.telegram.org/bot1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY/sendMessage?chat_id=-1001184175574&text='+ tabulate(result2, headers=['Stat', 'Closed', 'Current', 'Percent Change']))

    conn.commit()