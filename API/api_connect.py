from crypt import methods
from flask import Flask, request, jsonify
import json
import threading
import datetime
import pandas as pd
from time import sleep
from flask_cors import CORS, cross_origin

from Strategy.five_ema import FiveEMA
from Strategy.short_straddle import ShortStraddle
from Broker.main_broker import Zerodha
import settings

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

broker_instance = Zerodha()
five_ema_strategy_instance = FiveEMA(broker_instance)
short_straddle_strategy_instance = ShortStraddle(broker_instance)
short_straddle_thread = threading.Thread(target=short_straddle_strategy_instance.run_short_straddle)
short_straddle_thread.start()

def update_broker_instance_on_new_day():
    """
    Updates broker instance at the start of the day
    """
    global broker_instance
    while True:
        if datetime.datetime.now().time() >= datetime.time(9, 15, 15, 0) and datetime.datetime.now().time() <= datetime.time(9, 16, 0, 0):
            broker_instance = Zerodha()
        sleep(20)

broker_replacement_thread = threading.Thread(target=update_broker_instance_on_new_day)
broker_replacement_thread.start()


@app.route("/", methods=['GET'])
@cross_origin()
def check_server_active():
    return "SERVER RUNNING", 200

@app.route("/change_params", methods=['POST'])
@cross_origin()
def change_params():
    new_params = json.loads(request.data)
    new_properties = {
    "target": new_params['TARGET'],
    "trailing_stoploss": new_params['TRAILING_STOPLOSS'],
    "quantity": new_params['QUANTITY'],
    "lot_size": new_params['LOT_SIZE'],
    "stoploss": new_params['STOPLOSS'],
    "paper_trading": new_params['PAPER_TRADING']
    }   
    with open(settings.ACTION_PROPERTIES_FILE, 'w') as file:
        file.write(json.dumps(new_properties, indent=4))
    return "PARAMS UPDATED", 200

@app.route("/make_bot_active", methods=['POST'])
@cross_origin()
def make_bot_active():
    global five_ema_strategy_instance
    bot_status = json.loads(request.data)
    if bot_status['STATUS'] == "ACTIVE":
        strategy_instance_thread = threading.Thread(target=five_ema_strategy_instance.run_5ema)
        strategy_instance_thread.start()
    else:
        if five_ema_strategy_instance != None:
            five_ema_strategy_instance.strategy_active_flag = False
    return "BOT STATUS UPDATED", 200

@app.route("/get_bot_status", methods=['GET'])
def get_bot_status():
    global five_ema_strategy_instance
    if five_ema_strategy_instance == None:
        return "0", 200
    if five_ema_strategy_instance.strategy_active_flag == True:
        return "1", 200
    else:
        return "0", 200

@app.route('/fetch_attributes', methods=['GET'])
@cross_origin()
def fetch_attributes():
    global broker_instance

    month_mapping = {1:"JAN", 2:"FEB", 3:"MAR", 4:"APR", 5:"MAY", 6:"JUN", 7:"JUL", 8:"AUG", 9:"SEP", 10:"OCT", 11:"NOV", 12:"DEC"}
    month = month_mapping[datetime.date.today().month]
    year = (datetime.date.today().year)%100
    tradingsymbol = f"BANKNIFTY{year}{month}FUT"
    if broker_instance.check_trading_symbol(tradingsymbol) != True:
        month = month_mapping[(datetime.date.today().month + 1)%12]
        if datetime.date.today().month == 12:
            year = year+1
    
    response = {
        "FUTURE": ["BANKNIFTY FUT"],
        "EXPIRY": [f"{month} {year}"],
        "CANDLE_TIME": ["5 MINUTE"],
        "BUY_OR_SELL": ["BUY"],
        "STRATEGY": ['Five EMA']
    }
    return jsonify(response), 200

@app.route('/positions', methods=['GET'])
@cross_origin()
def fetch_positions():
    global five_ema_strategy_instance, short_straddle_strategy_instance
    response_five_ema = five_ema_strategy_instance.get_positions()
    response_short_straddle = short_straddle_strategy_instance.get_positions()
    response = response_five_ema + response_short_straddle
    response = [x for x in response if x != None]
    return jsonify(response), 200

@app.route('/tradebook', methods=['GET'])
@cross_origin()
def fetch_tradebook():
    import csv
    with open(settings.CSV_LOGS_FILE, mode='r') as infile:
        reader = csv.reader(infile)
        counter = 0
        response = []

        keys = ['orderID', 'dateTimes', 'instrumentType', 'orderType', 'quantity', 'target', 'stoploss', 'trailingSL', 'bankFutPrice', 'paperTrade']
        for row in reader:
            if counter == 0:
                pass
            else:
                new_dict = {}
                for i in range(len(keys)):
                    new_dict[keys[i]] = row[i]
                response.append(new_dict)
            counter += 1

    json_response = json.dumps(response, indent=4)
    print(json_response)
    return json_response, 200

    


