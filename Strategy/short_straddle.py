# SYSTEM
import logging
import os
import datetime
import threading
from time import sleep
import csv

#DATA
import json

# CUSTOM 
import settings
from Broker.main_broker import Zerodha


class ShortStraddle:
    def __init__(self, broker:Zerodha):
        self.__broker = broker
        if self.__broker == None:
            exit(1)
        self.logger = self.get_logger()

        self.running_trades = [None, None] # [{STRATEGY, DATE TIME, ORDER_ID, TRADING_SYMBOL, BANKNIFTY FUT LTP, QUANTITY, ENTRY PRICE, STATUS}]

        self.month_mapping = {1:"JAN", 2:"FEB", 3:"MAR", 4:"APR", 5:"MAY", 6:"JUN", 7:"JUL", 8:"AUG", 9:"SEP", 10:"OCT", 11:"NOV", 12:"DEC"}

        month = self.month_mapping[datetime.date.today().month]
        year = (datetime.date.today().year)%100
        tradingsymbol = f"BANKNIFTY{year}{month}FUT"
        if self.__broker.check_trading_symbol(tradingsymbol) == True:
            self.bank_nifty_fut_instrument_token = self.__broker.get_instrument_token(tradingsymbol)
        else:
            month = self.month_mapping[(datetime.date.today().month + 1)%12]
            if datetime.date.today().month == 12:
                year = year+1
            tradingsymbol = f"BANKNIFTY{year}{month}FUT"
            self.bank_nifty_fut_instrument_token = self.__broker.get_instrument_token(tradingsymbol)

        # Create Excel Order Log
        if not os.path.isfile(settings.SHORT_STRADDLE_ORDER_LOG_FILE):
            with open(settings.SHORT_STRADDLE_ORDER_LOG_FILE, "a") as file:
                field_names = ["ORDER ID", "DATE TIME", "INSTRUMENT TOKEN", "ORDER TYPE", "QUANTITY", "BNF PRICE", "ATM PRICE"]
                writer = csv.DictWriter(file, fieldnames=field_names)
                writer.writeheader()
    
    def get_logger(self):
        """
        Creates a logger with stream and file handlers, and returns it. 
        """
        logger = logging.getLogger('Short Straddle Logger')
        logger.setLevel(logging.DEBUG)
        c_handler = logging.StreamHandler()
        f_handler = logging.FileHandler(os.path.join(settings.LOGS_FOLDER, "ShortStraddle.log"))
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

    def get_atm(self, price):
        """
        Returns selected BankNifty ATM PE for the price passed
        """
        if price%100 >= 50:
            price = ((price//100)+1)*100
        else:
            price = (price//100)*100

        bnf_fut = self.__broker.get_trading_symbol(self.bank_nifty_fut_instrument_token)[:-3]
        tradingsymbol_PE = f"{bnf_fut}{int(price)}PE"
        tradingsymbol_CE = f"{bnf_fut}{int(price)}CE"
        return tradingsymbol_CE, tradingsymbol_PE

    def get_positions(self):
        """
        Returns the current positions
        """
        return self.running_trades

    def close_position(self, ind=[], reason=[2, 2]):
        """
        ind : [[ce_token, atm_ce], [pe_token, atm_pe]]
        reason : 0 - Target, 1 - Stoploss, 2 - Time Trigger
        """
        reason_mapping = {0: "Target Reached", 1:"Stoploss Triggered", 2:"Time Trigger"}

        for counter in range(len(ind)):
            item = ind[counter]
            self.logger.info(f"""
            ORDER ID : PAPER TRADE
            ORDER TYPE : BUY
            TRADING SYMBOL : {item[1]}
            BANKNIFTY FUT PRICE : {self.__broker.live_data_dictionary[self.bank_nifty_fut_instrument_token]}
            ENTRY PRICE : {self.__broker.live_data_dictionary[item[0]]}
            QUANTITY : 1
            REASON : {reason_mapping[reason[counter]]}
            """)

    def update_broker_instance(self, broker):
        """
        Updates new broker instance
        """
        self.__broker = broker

    def run_short_straddle(self):
        """
        Run short straddle strategy unless explicitely stopped
        """    
        self.logger.info("5EMA strategy started...")
        self.strategy_active_flag = True
        lot_size = 25

        while True:
            self.logger.info("Waiting for market to start ...")
            while datetime.datetime.now().time() <= datetime.time(9, 16, 0, 0) or datetime.datetime.now().time() >= datetime.time(15, 30, 0, 0):
                sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)
                if self.strategy_active_flag == False:
                    return

            self.logger.info("Market in progress ...")

            while True: # Run this strategy unless stopped otherwise
                while datetime.datetime.now().time() <= datetime.time(9, 17, 0):
                    sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)
                
                if datetime.datetime.now().time() > datetime.time(9, 18, 0):
                    break
                
                self.logger.info("Strategy executed, time : 09:17")
                bnf_price = self.__broker.live_data_dictionary[self.bank_nifty_fut_instrument_token]
                atm_ce, atm_pe = self.get_atm(bnf_price)
                ce_token = self.__broker.get_instrument_token(atm_ce)
                pe_token = self.__broker.get_instrument_token(atm_pe)
                self.__broker.subscribe_instruments([ce_token, pe_token])
                sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)

                # =================================================================================================
                # Execute orders
                #  [{STRATEGY, DATE TIME, ORDER_ID, TRADING_SYMBOL, BANKNIFTY FUT LTP, QUANTITY, ENTRY PRICE, STATUS}]
                self.logger.info(f"""
                ORDER ID : PAPER TRADE
                ORDER TYPE : SELL
                TRADING SYMBOL : {atm_ce}
                BANKNIFTY FUT PRICE : {bnf_price}
                ENTRY PRICE : {self.__broker.live_data_dictionary[ce_token]}
                QUANTITY : 1
                """)

                self.logger.info(f"""
                ORDER ID : PAPER TRADE
                ORDER TYPE : SELL
                TRADING SYMBOL : {atm_pe}
                BANKNIFTY FUT PRICE : {bnf_price}
                ENTRY PRICE : {self.__broker.live_data_dictionary[pe_token]}
                QUANTITY : 1
                """)

                d = {"STRATEGY": "SHORT STRADDLE", "DATE TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ORDER ID": "PAPER TRADE", "BANKNIFTY FUT LTP": bnf_price, "QUANTITY": "1", 
                "ENTRY PRICE": self.__broker.live_data_dictionary[ce_token], "STATUS": "ACTIVE",
                "TRADING SYMBOL": atm_ce
                }
                self.running_trades[0] = d
                d = {"STRATEGY": "SHORT STRADDLE", "DATE TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "ORDER ID": "PAPER TRADE", "BANKNIFTY FUT LTP": bnf_price, "QUANTITY": "1", 
                "ENTRY PRICE": self.__broker.live_data_dictionary[pe_token], "STATUS": "ACTIVE",
                "TRADING SYMBOL": atm_pe
                }

                # =========================================================================================================
                # EXCEL UPLOAD
                # "ORDER ID", "DATE TIME", "INSTRUMENT TOKEN", "ORDER TYPE", "QUANTITY", "BNF PRICE", "ATM PRICE"
                excel_log_ce = {
                    "ORDER ID": "PAPER_TRADE",
                    "DATE TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ORDER TYPE": "SELL",
                    "INSTRUMENT TOKEN": atm_ce, 
                    "QUANTITY": 1*lot_size,
                    "BNF PRICE": bnf_price,
                    "ATM PRICE": self.__broker.live_data_dictionary[ce_token]
                    }
                excel_log_pe = {
                    "ORDER ID": "PAPER_TRADE",
                    "DATE TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ORDER TYPE": "SELL",
                    "INSTRUMENT TOKEN": atm_ce, 
                    "QUANTITY": 1*lot_size,
                    "BNF PRICE": bnf_price,
                    "ATM PRICE": self.__broker.live_data_dictionary[pe_token]
                    }
                with open(settings.SHORT_STRADDLE_ORDER_LOG_FILE, "a") as file:
                    writer = csv.DictWriter(file, fieldnames=list(excel_log_ce.keys()))
                    writer.writerows([excel_log_ce, excel_log_pe])


                self.running_trades[1] = d
                while datetime.datetime.now().time() <= datetime.time(14, 55, 0, 0):
                    ce_ltp = self.__broker.live_data_dictionary[ce_token]
                    pe_ltp = self.__broker.live_data_dictionary[pe_token]
                    if self.running_trades[0] != None and ce_ltp <= self.running_trades[0]['ENTRY PRICE'] - 2500/lot_size:   # CE TARGET REACHED
                        self.close_position(ind=[[ce_token, atm_ce], [pe_token, atm_pe]], reason=[0, 0])
                        self.running_trades = [None, None]
                        
                    elif self.running_trades[0] != None and ce_ltp >= self.running_trades[0]['ENTRY PRICE'] + 2500/lot_size: # CE STOPLOSS TRIGGERED
                        self.close_position(ind=[[ce_token, atm_ce], [pe_token, atm_pe]], reason=[1, 1])
                        self.running_trades = [None, None]

                    if self.running_trades[1] != None and pe_ltp <= self.running_trades[1]['ENTRY PRICE'] - 2500/lot_size:   # PE TARGET TRIGGERED
                        self.close_position(ind=[[pe_token, atm_pe], [ce_token, atm_ce]], reason=[0, 0])
                        self.running_trades = [None, None]

                    elif self.running_trades[1] != None and pe_ltp >= self.running_trades[1]['ENTRY PRICE'] + 2500/lot_size: # PE STOPLOSS TRIGGERED
                        self.close_position(ind=[[pe_token, atm_pe], [ce_token, atm_ce]], reason=[1, 1])
                        self.running_trades = [None, None]

                    if self.running_trades == [None, None]:
                        self.logger.info("Short straddle trade completed for today")
                        break

                    sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)

                excel_log_ce = {
                    "ORDER ID": "PAPER_TRADE",
                    "DATE TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ORDER TYPE": "BUY",
                    "INSTRUMENT TOKEN": atm_ce, 
                    "QUANTITY": 1*lot_size,
                    "BNF PRICE": bnf_price,
                    "ATM PRICE": self.__broker.live_data_dictionary[ce_token]
                    }
                excel_log_pe = {
                    "ORDER ID": "PAPER_TRADE",
                    "DATE TIME": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "ORDER TYPE": "BUY",
                    "INSTRUMENT TOKEN": atm_ce, 
                    "QUANTITY": 1*lot_size,
                    "BNF PRICE": bnf_price,
                    "ATM PRICE": self.__broker.live_data_dictionary[pe_token]
                    }
                with open(settings.SHORT_STRADDLE_ORDER_LOG_FILE, "a") as file:
                    writer = csv.DictWriter(file, fieldnames=list(excel_log_ce.keys()))
                    writer.writerows([excel_log_ce, excel_log_pe])

                if self.running_trades[0] != None:
                    self.close_position(ind=[[ce_token, atm_ce], [pe_token, atm_pe]], reason=[2, 2])
                break

            self.logger.info("Waiting for market to end")
            while datetime.datetime.now().time() <= datetime.time(15, 30, 0, 0):
                sleep(settings.SLEEP_TIME_BETWEEN_ATTEMPTS)
            
