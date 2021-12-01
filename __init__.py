import logging
import os
from time import sleep
import threading
import websocket, json
import alpaca_trade_api as tradeapi
import pandas as pd

# init
logging.basicConfig(
	filename='errlog.log',
	level=logging.WARNING,
	format='%(asctime)s:%(levelname)s:%(message)s',
)

#ConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariables
#ConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariables
#Twitter API KEYS
twitter_consumer_key        = "KqGmXU8Bo1IdpBzq824HQVEBe",
twitter_consumer_secret     = "ELJJeSwnDUScsm1UFcWjFJAal7lw2YhyyMJqUTDYCXtrn0sbaR",
twitter_access_token        = "28787053-9RJW0D324Lic7ChT1l7hf52BeP4GpJoycsfNxnveT",
twitter_access_token_secret = "dGRQBOz0X3QeVZuJIu1p2w9X5A3hbcSq65OKByAxUGCly"

#Telegram Bot API Token
telegramAPItoken            = "1556748550:AAEIqPHM4ZIO-k-k8TgKe1uJYJfS2hb5vfY"

#Alpaca Trading
alpaca_api_key = 'PKPCPT880HFV8B92PBMI'
alpaca_api_secret = 'wx3YP0ppt762PPwP8czFgX9NY0DdiIPEWsZfqGrU'
alpaca_base_url = 'https://paper-api.alpaca.markets'
alpaca_data_url = 'wss://data.alpaca.markets'

#Script Configurations
max_results  = 1500
cron_job = "*/10 * * * * /Library/Frameworks/Python.framework/Versions/3.8/bin/python3 /Users/xstjames/Code/Development/ryze/engine.p"

#File Path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

#Database Path
db_path = os.path.join(BASE_DIR, "ryze.db")
#ConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariables
#ConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariablesConfigVariables

# instantiate REST API
alpaca_api = tradeapi.REST(alpaca_api_key, alpaca_api_secret, alpaca_base_url, api_version='v2')

# init WebSocket
alpaca_conn = tradeapi.stream2.StreamConn(
	'alpaca_api_key',
	'alpaca_api_secret',
	base_url=alpaca_base_url,
	data_url=alpaca_base_url,
	data_stream='alpacadatav1',
)

def time_to_market_close():
	clock = alpaca_api.get_clock()
	return (clock.next_close - clock.timestamp).total_seconds()


def wait_for_market_open():
	clock = alpaca_api.get_clock()
	if not clock.is_open:
		time_to_open = (clock.next_open - clock.timestamp).total_seconds()
		sleep(round(time_to_open))


def set_trade_params(df):
	return {
		'high': df.high.tail(10).max(),
		'low': df.low.tail(10).min(),
		'trade_taken': False,
	}


def send_order(direction, bar):
	if time_to_market_close() > 120:
		print(f'sent {direction} trade')
		range_size = trade_params['high'] - trade_params['low']

		if direction == 'buy':
			sl = bar.high - range_size
			tp = bar.high + range_size
		elif direction == 'sell':
			sl = bar.low + range_size
			tp = bar.low - range_size

			alpaca_api.submit_order(
			symbol='SNDL',
			qty=100,
			side=direction,
			type='market',
			time_in_force='day',
			order_class='bracket',
			stop_loss=dict(stop_price=str(sl)),
			take_profit=dict(limit_price=str(tp)),
		)

		return True

	wait_for_market_open()
	return False


@alpaca_conn.on(r'^AM.SNDL$')
async def on_minute_bars(conn, channel, bar):
	if isinstance(candlesticks.df, pd.DataFrame):
		ts = pd.to_datetime(bar.timestamp, unit='ms')
		candlesticks.df.loc[ts] = [bar.open, bar.high, bar.low, bar.close, bar.volume]

	if not trade_params['trade_taken']:
		if bar.high > trade_params['high']:
			trade_params['trade_taken'] = send_order('buy', bar)

		elif bar.low < trade_params['low']:
			trade_params['trade_taken'] = send_order('sell', bar)

	if time_to_market_close() > 120:
		wait_for_market_open()


@alpaca_conn.on(r'^trade_updates$')
async def on_trade_updates(conn, channel, trade):
	if trade.order['order_type'] != 'market' and trade.order['filled_qty'] == '100':
		# trade closed - look for new trade
		trade_params = set_trade_params(candlesticks.df.SNDL)


candlesticks = alpaca_api.get_barset('SNDL', 'minute', limit=10)
trade_params = set_trade_params(candlesticks.df.SNDL)
alpaca_conn.run(['AM.SNDL', 'trade_updates'])



def on_open(ws):
    print("opened")
    auth_data = {
        "action": "authenticate",
        "data": {"key_id": alpaca_api_key, "secret_key": alpaca_api_secret}
    }

    ws.send(json.dumps(auth_data))

    listen_message = {"action": "listen", "data": {"streams": ["AM.SNDL", 'AM.TSLA']}}

    ws.send(json.dumps(listen_message))


def on_message(ws, message):
    print("received a message")
    print(message)

def on_close(ws):
    print("closed connection")

socket = "wss://data.alpaca.markets/stream"

ws = websocket.WebSocketApp(socket, on_open=on_open, on_message=on_message, on_close=on_close)
ws.run_forever()