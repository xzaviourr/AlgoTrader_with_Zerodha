# SYSTEM
import logging
import os
import datetime
import threading
from time import sleep

#DATA
import pandas_ta as ta
import json

# CUSTOM 
from Broker.main_broker import Zerodha
import settings


class FiveEMA:
    def __init__(self, broker:Zerodha):
        self.__broker = broker
        if self.__broker == None:
            exit(1)
        self.logger = self.get_logger()

        self.month_mapping = {1:"JAN", 2:"FEB", 3:"MAR", 4:"APR", 5:"MAY", 6:"JUN", 7:"JUL", 8:"AUG", 9:"SEP", 10:"OCT", 11:"NOV", 12:"DEC"}
        self.last_fetched_record_time = None
        self.strategy_active_flag = False

        self.trade_region = False
        self.trigger_candle = None

        try:
            with open(settings.ACTION_PROPERTIES_FILE) as file:
                self.action_properties = json.load(file)
            self.logger.info("Action properties successfully loaded")
        except Exception as e:
            self.logger.critical("Action properties file cannot be found. Application exiting.\n")
    
    def get_logger(self):
        """
        Creates a logger with stream and file handlers, and returns it. 
        """
        logger = logging.getLogger('FiveEMA Logger')
        logger.setLevel(logging.DEBUG)
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler(os.path.join(settings.LOGS_FOLDER, "FiveEMA.log"))
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

    def get_ema(self, market_data):
        """
        Returns EMA field in the dataframe provided
        """
        no_of_records_fetched = market_data.shape[0]
        no_of_records_fetched = min(no_of_records_fetched, 5)
        market_data['EMA'] = ta.ema(market_data['close'], length=no_of_records_fetched)

    def get_atm_pe(self, price):
        """
        Returns selected BankNifty ATM PE for the price passed
        """
        if price%100 >= 50:
            price = ((price//100)+1)*100
        else:
            price = (price//100)*100

        bnf_fut = self.__broker.get_trading_symbol(self.__broker.bank_nifty_fut_instrument_token)[:-3]
        tradingsymbol = f"{bnf_fut}{int(price)}PE"
        return tradingsymbol

    def get_positions(self):
        """
        Returns the current positions
        """
        return self.__broker.get_positions()

    def update_broker_instance(self, broker):
        """
        Updates new broker instance
        """
        self.__broker = broker

    def run_5ema(self):
        """
        Run 5EMA strategy unless explicitely stopped
        """    
        self.logger.info("5EMA strategy started...")
        self.strategy_active_flag = True

        while True:
            self.logger.info("Waiting for market to start ...")
            while datetime.datetime.now().time() <= datetime.time(9, 16, 0, 0) or datetime.datetime.now().time() >= datetime.time(14, 30, 0, 0):
                sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)
                if self.strategy_active_flag == False:
                    return
            self.logger.info("Market in progress ...")

            while True: # Run this strategy unless stopped otherwise
                self.logger.info("Waiting for the next candle ...")
                mins = datetime.datetime.now().time().minute
                nearest_interval = ((mins//5 + 1)*5)%60
                while(datetime.datetime.now().time().minute != nearest_interval):    # Wait till the nearest 5 min interval
                    sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)

                if datetime.datetime.now().time() >= datetime.time(14, 30, 0, 0):  # Market Ended, so stop the strategy
                    self.strategy_active_flag = False
                    self.logger.info("Five EMA strategy stopped till next day. Market Closing.")
                    break
                
                while True:
                    if self.strategy_active_flag == 0:  # Strategy stopped by external event
                        return

                    market_data = self.__broker.fetch_BNF_historical_data()
                    latest_record_time = market_data['date'].iloc[-1]

                    if latest_record_time != self.last_fetched_record_time:  # New data available now
                        self.last_fetched_record_time = latest_record_time
                        break
                    sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)

                self.logger.info(f"TIME : {datetime.datetime.now()}\nNew Candle Fetched\n Candle TimeStamp : {latest_record_time}")

                self.get_ema(market_data)   # Get values of EMA
                new_candle = market_data.iloc[-1]

                # STRATEGY
                # ===========================================================
                if self.trade_region == False:  # If I am currently out of the trade region
                    if self.__broker.is_active_trade == True:   # Trade is already running
                        self.last_candle = new_candle
                    elif new_candle['low'] > new_candle['EMA']:
                        self.logger.info("Entered Trade Region")
                        self.logger.info(f"Current Candle Low : {new_candle['low']} | Current EMA : {new_candle['EMA']}")
                        self.trigger_candle = new_candle    # New trigger candle
                        self.last_candle = new_candle   # Last candle
                        self.trade_region = True    # Moved into the trade region
                    else:
                        self.logger.info("Candle below EMA, out of trade region")
                        self.logger.info(f"Current Candle Low : {new_candle['low']} | Current EMA : {new_candle['EMA']}")

                else:   # If I am currently in the trade region
                    if new_candle['close'] < self.trigger_candle['low']:    # Execute order
                        self.logger.info("Order Executing")
                        self.logger.info(f"Current Candle Close : {new_candle['close']} | Trigger Candle Low : {self.trigger_candle['low']}")
                        
                        # Fetch all the action properties
                        lot_size = self.action_properties['lot_size']
                        qty = self.action_properties['quantity']
                        target = self.action_properties['target']
                        stoploss = min(float(self.action_properties['stoploss']), float(self.trigger_candle['high'] - new_candle['close']))
                        trailingSL = self.action_properties['trailing_stoploss']
                        paper_trading = True if self.action_properties['paper_trading'] == 1 else False
                        
                        tradingsymbol = self.get_atm_pe(new_candle['close'])    # ATM PE TRADING SYMBOL

                        self.__broker.place_buy_order(
                            tradingsymbol = tradingsymbol,
                            quantity = lot_size * qty,
                            target = new_candle['close'] + min(target, 3*stoploss), 
                            stoploss = stoploss,
                            trailingSL = trailingSL,
                            price = new_candle['close'],
                            paper_trading=paper_trading
                        )
                        self.close_position_thread = threading.Thread(target=self.__broker.close_position)
                        self.close_position_thread.start()

                        self.trade_region = False   # Come out of trade region

                    elif new_candle['close'] > self.trigger_candle['low']:  # Shift to new trigger candle
                        if new_candle['low'] > new_candle['EMA'] and new_candle['low'] > self.last_candle['low']:
                            self.logger.info("Trigger candle shifted")
                            self.logger.info(f"Low : {new_candle['low']} EMA : {new_candle['EMA']} Last Low : {self.last_candle['low']}")
                            self.trigger_candle = new_candle
                        else:
                            self.logger.info("EMA touching candle, Waiting for next one ..")
                    self.last_candle = new_candle
