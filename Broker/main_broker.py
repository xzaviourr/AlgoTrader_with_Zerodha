# SYSTEM
import os
from sys import exc_info
import datetime
import logging
from time import sleep, time

# WEB
import requests
import pyotp
from urllib import parse
from kiteconnect import KiteConnect, KiteTicker

# DATA
import pandas as pd
import csv
import json

# CUSTOM
import settings

class Zerodha:
    """
    Object for Zerodha broker, contains all broker functions for 5EMA strategy
    """
    def __init__(self):
        # BROKER CONNECTION VARIABLES
        self.__conn = None  # Broker connection object
        self.__ticker = None  # Broker ticker object

        # DYNAMIC TRADE DATA
        self.live_data_dictionary = {}  # Contains dynamic values for the particular token - {instrument_token : LTP}
        self.active_trade = None # Trade that needs to be closed with SL or target - {order_id, instrument_token, quantity, target, stoploss, trailingSL, price, paper_trade}
        self.is_active_trade = False

        # STATIC VARIABLES
        self.month_mapping = {1:"JAN", 2:"FEB", 3:"MAR", 4:"APR", 5:"MAY", 6:"JUN", 7:"JUL", 8:"AUG", 9:"SEP", 10:"OCT", 11:"NOV", 12:"DEC"}

        # UTILITY VARIABLES
        self.logger = self.get_logger()
        self.instruments = self.load_instruments()

        # Broker login initiation
        self.__conn, self.__ticker = self.login() 
        if type(self.__conn) == int:
            exit(1)

        # Create Excel Order Log
        if not os.path.isfile(settings.CSV_LOGS_FILE):
            with open(settings.CSV_LOGS_FILE, "a") as file:
                field_names = ["ORDER ID", "DATE TIME", "INSTRUMENT TOKEN", "ORDER TYPE", "QUANTITY", "TARGET", "STOPLOSS", "TRAILING SL", "BANKNIFTY FUT PRICE", "PAPER TRADE"]
                writer = csv.DictWriter(file, fieldnames=field_names)
                writer.writeheader()

        # Start live streaming of Data
        self.__ticker.connect(threaded=True)
        start_time = time()
        while(self.__ticker.is_connected() == False): # Wait till streaming becomes live
            sleep(0.2)
            if time() - start_time > settings.TICKER_RETRY_TIMEOUT:
                self.logger.critical("Live streaming cannot be started. Increase TICKER_RETRY_TIMEOUT for weaker networks. Appliation exiting ..\n")
                exit(1)

    def get_logger(self):
        """
        Creates a logger with stream and file handlers, and returns it. 
        """
        logger = logging.getLogger('Zerodha Logger')
        logger.setLevel(logging.DEBUG)
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler(os.path.join(settings.LOGS_FOLDER, "zerodha.log"))
        c_handler.setLevel(logging.DEBUG)
        f_handler.setLevel(logging.DEBUG)
        c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)
        logger.info("Logger initialized")
        return logger

    def login(self):
        """
        Creates a client object after performing authentication with zerodha. This client object
        can be used to take actions on the user account. 
        
        Returns:
            client and ticker objects
        """
        self.logger.info("Starting Broker Login Process ..")
        BROKER_LOGIN_ATTEMPT_COUNT = 0
        while BROKER_LOGIN_ATTEMPT_COUNT < settings.MAX_BROKER_LOGIN_ATTEMPT_COUNT:
            try:
                try:
                    with open(settings.BROKER_CREDENTIALS_FILE) as file:
                        credentials = json.load(file)
                except Exception as e:
                    self.logger.critical("Broker credentials file not found ..\n", exc_info=True)
                    return

                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77"}
                session = requests.Session()
                session.headers.update(headers)
                
                get_response = session.get(f"https://kite.trade/connect/login?api_key={credentials['api_key']}")
                login_response = json.loads(session.post("https://kite.zerodha.com/api/login", data={
                    'user_id': credentials['user_id'],
                    'password': credentials['password']
                }).text)

                totp = pyotp.TOTP(credentials['totp_code'])
                totp_response = session.post("https://kite.zerodha.com/api/twofa",
                    data = {
                        'user_id': credentials['user_id'],
                        'request_id': login_response['data']['request_id'],
                        'twofa_value': totp.now()
                    }
                )

                # Extracting the token
                token = ""
                try:
                    final_response = session.get(get_response.url + "&skip_session=true")
                    parsed_text = parse.urlparse(final_response.history[1].headers['location'])
                    token = parse.parse_qs(parsed_text.query)['request_token'][0]
                except Exception as e:
                    self.logger.critical("Error in generating zerodha token\n", exc_info=True)
                    return

                session.close()
                self.__conn = KiteConnect(api_key=credentials['api_key'])
                data = self.__conn.generate_session(
                    request_token=token,
                    api_secret=credentials['api_secret']
                )
                
                self.__conn.set_access_token(data['access_token'])

                # ==============================================================================
                # TICKER
                self.__ticker = KiteTicker(
                    api_key=credentials['api_key'],
                    access_token=data['access_token']
                )
                self.__ticker.on_close = self.on_close 
                self.__ticker.on_ticks = self.on_ticks
                self.__ticker.on_connect = self.on_connect
                self.__ticker.on_error = self.on_error

                if data['access_token'] == None:
                    return self.__conn, self.__ticker
                else:
                    self.logger.info("Broker Login Successful")
                    return self.__conn, self.__ticker
            except Exception as e:
                self.logger.error("Broker login failed. Retrying ..", exc_info=True)
            BROKER_LOGIN_ATTEMPT_COUNT += 1
        self.logger.critical("Broker login max retries exceeded. Application exiting ..")
        return 1, 1

    def load_instruments(self):
        """
        Loads the instruments from the instruments file. If file not exists or has gotten old, 
        new file is downloaded from the web.
        """
        FETCH_INSTRUMENT_ATTEMPT_COUNTER = 0
        while FETCH_INSTRUMENT_ATTEMPT_COUNTER < settings.MAX_FETCH_INSTRUMENT_FILE_ATTEMPT_COUNT:
            try:
                file_path = settings.INSTRUMENTS_FILE
                if os.path.exists(file_path):
                    m_dt = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                    m_dt = m_dt.date()  # Extracting date
                    
                if not os.path.exists(file_path) or m_dt != datetime.datetime.now().date():  # If file not exists or file was not modified today
                    url = "https://api.kite.trade/instruments"
                    response = requests.get(url, allow_redirects=True)
                    open(file_path, 'wb').write(response.content)
                
                with open(file_path, 'r') as file:
                    instruments = pd.read_csv(file)
                self.logger.info("Instruments loaded successfully")
                return instruments
            except Exception as e:
                self.logger.error("Instrument file download failed. Retrying..", exc_info=True)
                sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)
            FETCH_INSTRUMENT_ATTEMPT_COUNTER += 1
        self.logger.critical("Instrument file download max retries exceeded. Application exiting ..")
        exit(1)

    def get_exchange(self, tradingsymbol):
        """
        Returns Exchange by mapping the input symbol name, -1 in case of failure
        """
        exch = -1
        try:
            exch = str(self.instruments[self.instruments['tradingsymbol'] == tradingsymbol]['exchange'].iloc[0])
        except Exception as e:
            self.logger.error("Failed to fetch Exchange .. \n", exc_info=True)
        return exch

    def get_instrument_token(self, tradingsymbol):
        """
        Returns instrument token by mapping the input symbol name, -1 in case of failure
        """
        instrument_token = -1
        try:
            instrument_token = int(self.instruments[self.instruments['tradingsymbol'] == tradingsymbol]['instrument_token'].iloc[0])
        except Exception as e:
            self.logger.error(f"Failed to fetch instrument token for {tradingsymbol}.. \n", exc_info=True)
        return instrument_token

    def get_trading_symbol(self, instrument_token):
        """
        Returns trading symbol by mapping the input trading instrument, -1 in case of failure
        """
        trading_symbol = -1
        try:
            trading_symbol = str(self.instruments[self.instruments['instrument_token'] == instrument_token]['tradingsymbol'].iloc[0])
        except Exception as e:
            self.logger.error("Failed to fetch trading symbol .. \n", exc_info=True)
        return trading_symbol

    def check_trading_symbol(self, tradingsymbol):
        """
        Returns true if trading symbol exists
        """
        try:
            instrument_token = int(self.instruments[self.instruments['tradingsymbol'] == tradingsymbol]['instrument_token'].iloc[0])
            return True
        except Exception as e:
            return False

    def fetch_BNF_historical_data(self):
        """
        Returns Bank Nifty Fut historical data of current day
        with 5 min interval in the form of pandas dataframe
        """
        RETRY_COUNT = 0
        while RETRY_COUNT < settings.HISTORICAL_DATA_FETCH_MAX_RETRY:
            try:
                data = self.__conn.historical_data(
                    instrument_token = self.bank_nifty_fut_instrument_token,
                    from_date = datetime.date.today().strftime("%Y-%m-%d"),
                    to_date = datetime.date.today().strftime("%Y-%m-%d"),
                    interval = "5minute"
                )
                return pd.DataFrame(data)
            except Exception as e:
                self.logger.error("Error in fetching BNF historical data. Retrying ..", exc_info=True)
                sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)
                RETRY_COUNT += 1
                
        self.logger.critical("Historical data fetch retry limited exceeded. Application exiting ..")
        exit(1)
        
    def place_buy_order(self, tradingsymbol, quantity, target, stoploss, trailingSL, price, paper_trading:False):
        """
        Places buy order for the provided trading symbol with the given parameters
        """
        self.is_active_trade = True

        if paper_trading == True:
            instrument_token = self.get_instrument_token(tradingsymbol)
            self.logger.info(f"BUY TRADE TRIGGERED\nOrder ID: PAPER_TRADE\nInstrument Token: {instrument_token}\nQuantity: {quantity}\nTarget: {target}\nStoploss: {stoploss}\nTrailingSL: {trailingSL}\nPrice: {price}")
            order_id = "PAPER_TRADE"
        
        else:
            ORDER_PLACE_COUNTER = 0
            while ORDER_PLACE_COUNTER < settings.MAX_ORDER_PLACEMENT_RETRIES:
                ORDER_PLACE_COUNTER += 1
                try:
                    order_id = self.__conn.place_order(
                        variety = "regular", 
                        exchange = "NSE", 
                        tradingsymbol = tradingsymbol, 
                        transaction_type = "BUY", 
                        quantity = quantity, 
                        product = "MIS", 
                        order_type = "MARKET", 
                        price = None
                        )
                except Exception as e:
                    self.logger.error("Error placing order on zerodha ..", exc_info=True)
                    continue

                ORDER_STATUS_CHECK_ATTEMPTS = 0
                order_status = self.__conn.order_history(order_id=order_id)[-1]['status']
                while order_status not in ["COMPLETE", "CANCELLED", "REJECTED"] and ORDER_STATUS_CHECK_ATTEMPTS < 10:  # Wait till order reaches on of these states
                    sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)
                    ORDER_STATUS_CHECK_ATTEMPTS += 1
                    order_status = self.__conn.order_history(order_id=order_id)[-1]['status']

                if order_status == "COMPLETE":  # Trade executed
                    instrument_token = self.get_instrument_token(tradingsymbol)
                    self.logger.info(f"BUY TRADE TRIGGERED\nOrder ID: {order_id}\nInstrument Token: {instrument_token}\nQuantity: {quantity}\nTarget: {target}\nStoploss: {stoploss}\nTrailingSL: {trailingSL}\nPrice: {price}")
                    break
                else:
                    self.logger.error(f"Trade Status {order_status}")
            
            if ORDER_PLACE_COUNTER >= settings.MAX_ORDER_PLACEMENT_RETRIES:
                self.logger.critical("Order placment max retries exceeded. Application exiting..")
                exit(1)

        self.active_trade = {
            "date_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "order_id": order_id,
            "instrument_token": instrument_token, 
            "quantity": quantity,
            "target": target,
            "stoploss": stoploss,
            "trailingSL": trailingSL,
            "price": price,
            "paper_trade": True
            }

        excel_log = {
            "ORDER ID": "PAPER_TRADE",
            "DATE TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ORDER TYPE": "BUY",
            "INSTRUMENT TOKEN": instrument_token, 
            "QUANTITY": quantity,
            "TARGET": target,
            "STOPLOSS": stoploss,
            "TRAILING SL": trailingSL,
            "BANKNIFTY FUT PRICE": price,
            "PAPER TRADE": paper_trading
            }
        with open(settings.CSV_LOGS_FILE, "a") as file:
            writer = csv.DictWriter(file, fieldnames=list(excel_log.keys()))
            writer.writerows([excel_log])

    def place_sell_order(self, tradingsymbol, quantity, price, paper_trading=False):
        """
        Places sell order for the given trading symbol with given parameters
        """
        self.is_active_trade = False
        if paper_trading == True:
            instrument_token = self.get_instrument_token(tradingsymbol)
            self.logger.info(f"SELL TRADE TRIGGERED\nOrder ID: PAPER_TRADE\nInstrument Token: {instrument_token}\nQuantity: {quantity}\nPrice: {price}")
            
        else:
            ORDER_PLACE_COUNTER = 0
            while ORDER_PLACE_COUNTER < settings.MAX_ORDER_PLACEMENT_RETRIES:
                ORDER_PLACE_COUNTER += 1
                try:
                    order_id = self.__conn.place_order(
                        variety = "regular", 
                        exchange = "NSE", 
                        tradingsymbol = tradingsymbol, 
                        transaction_type = "SELL", 
                        quantity = quantity, 
                        product = "MIS", 
                        order_type = "MARKET", 
                        price = None
                        )
                except Exception as e:
                    self.logger.error("Error placing order on zerodha ..", exc_info=True)
                    continue

                ORDER_STATUS_CHECK_ATTEMPTS = 0
                order_status = self.__conn.order_history(order_id=order_id)[-1]['status']
                while order_status not in ["COMPLETE", "CANCELLED", "REJECTED"] and ORDER_STATUS_CHECK_ATTEMPTS < 10:  # Wait till order reaches on of these states
                    sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)
                    ORDER_STATUS_CHECK_ATTEMPTS += 1
                    order_status = self.__conn.order_history(order_id=order_id)[-1]['status']
                
                if order_status == "COMPLETE":  # Trade executed
                    instrument_token = self.get_instrument_token(tradingsymbol)
                    self.logger.info(f"SELL TRADE TRIGGERED\nOrder ID: {order_id}\nInstrument Token: {instrument_token}\nQuantity: {quantity}\nPrice: {price}")
                    break
                else:
                    self.logger.error(f"Trade {order_status}. Retrying ..")
            
            if ORDER_PLACE_COUNTER >= settings.MAX_ORDER_PLACEMENT_RETRIES:
                self.logger.critical("Order placment max retries exceeded. Application exiting..")
                exit(1)

        excel_log = {
            "ORDER ID": "PAPER_TRADE",
            "DATE TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ORDER TYPE": "SELL",
            "INSTRUMENT TOKEN": instrument_token, 
            "QUANTITY": quantity,
            "TARGET": "NONE",
            "STOPLOSS": "NONE",
            "TRAILING SL": "NONE",
            "BANKNIFTY FUT PRICE": price,
            "PAPER TRADE": paper_trading
            }
        with open(settings.CSV_LOGS_FILE, "a") as file:
            writer = csv.DictWriter(file, fieldnames=list(excel_log.keys()))
            writer.writerows([excel_log])
        
        self.active_trade = None

    def close_position(self):
        """
        Keeps running indefinetly, keeping track of all the active trade of the user. If target or SL is triggered
        for that trade, position is closed off. 
        """
        while True:
            trade = self.active_trade
            ltp = self.live_data_dictionary[self.bank_nifty_fut_instrument_token]

            if ltp > trade['target'] or ltp <= trade['price'] - trade['stoploss'] or datetime.datetime.now().time() >= datetime.time(15, 30, 0, 0):
                self.place_sell_order(
                    tradingsymbol=self.get_trading_symbol(trade['instrument_token']),
                    quantity=trade['quantity'],
                    price=ltp,
                    paper_trading=trade['paper_trade']
                )
                self.active_trade = []
                if ltp > trade['target']:   # Target achieved
                    self.logger.info(f"Target achieved for order_id: {trade['order_id']}\nTrading symbol: {self.get_trading_symbol(trade['instrument_token'])}\nQuanity: {trade['quantity']}\nPrice: {ltp}")
                elif ltp <= trade['price'] - trade['stoploss']: # Stoploss triggered
                    self.logger.info(f"Stoploss triggered for order_id: {trade['order_id']}\nTrading symbol: {self.get_trading_symbol(trade['instrument_token'])}\nQuanity: {trade['quantity']}\nPrice: {ltp}")
                else:
                    self.logger.info("Exiting position due to market closure")
                return

            elif ltp > trade['price'] + trade['trailingSL']: # Move stoploss forward
                self.logger.info("Moved Stoploss forward")
                trade['price'] += trade['trailingSL']

            sleep(settings.DATA_UPDATE_TIME)

    def get_positions(self):
        """
        Returns list of live positions
        """
        if self.active_trade == None:
            return []
        else:
            response = [{
                "STRATEGY": "FIVE EMA",
                "DATE TIME": self.active_trade['date_time'],
                "ORDER_ID": self.active_trade['order_id'],
                "TRADING SYMBOL": self.get_trading_symbol(self.active_trade['instrument_token']),
                "BANKNIFTY FUT LTP": self.live_data_dictionary[self.bank_nifty_fut_instrument_token],
                "QUANTITY": self.active_trade['quantity'],
                "ENTRY PRICE": self.active_trade['price'],
                "STATUS": "ACTIVE"
            }]
            return response

    # =================================================================================================================
    # KITE Ticker
    def on_ticks(self, ws, ticks):
        """
        Called when new data is sent from the server. Updates the latest values of the tickers.
        """
        for instrument_data in ticks:
            token = instrument_data['instrument_token']
            ltp = instrument_data['last_price']        
            self.live_data_dictionary[token] = ltp  # Update the latest value of the ticker

    def on_connect(self, ws, response):
        """
        Called as soon as the socket is connected for streaming. Starts streaming of BankNifty FUT
        """
        month = self.month_mapping[datetime.date.today().month]
        year = (datetime.date.today().year)%100
        tradingsymbol = f"BANKNIFTY{year}{month}FUT"
        if self.check_trading_symbol(tradingsymbol) == True:
            self.bank_nifty_fut_instrument_token = self.get_instrument_token(tradingsymbol)
        else:
            month = self.month_mapping[(datetime.date.today().month + 1)%12]
            if datetime.date.today().month == 12:
                year = year+1
            tradingsymbol = f"BANKNIFTY{year}{month}FUT"
            self.bank_nifty_fut_instrument_token = self.get_instrument_token(tradingsymbol)

        ws.subscribe([self.bank_nifty_fut_instrument_token])
        self.logger.info("Socket connection successful. Started streaming ..")

    def on_close(self, ws, code, reason):
        """
        Called when the socket is closed for streaming
        """
        self.logger.error(f"Socket connection closed. Streaming stopped.\n{code} : {reason}")
        ws.stop()

    def on_error(self, ws, code, reason):
        """
        Called when socket encounters some errors and stops. 
        """
        self.logger.error(f"Socket streaming stopped due the error\n{code} : {reason}")
        ws.stop()

    def subscribe_instruments(self, instrument_tokens:list):
        """
        Subscribe list of instrument_tokens provided
        """
        self.logger.info(f"{instrument_tokens} Subscribed")
        self.__ticker.subscribe(instrument_tokens)

    def unsubscribe_instruments(self, instrument_tokens:list):
        """
        Unsubscribe the list of instrument_tokens provided
        """
        self.logger.info(f"{instrument_tokens} unsubscribed")
        self.__ticker.unsubscribe(instrument_tokens)

