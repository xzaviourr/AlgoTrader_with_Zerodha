import os
import json

# GLOBAL PATHS
# ===========================================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BROKER_DIR = os.path.join(BASE_DIR, "Broker")
STRATEGY_DIR = os.path.join(BASE_DIR, "Strategy")

BROKER_CREDENTIALS_FILE = os.path.join(BROKER_DIR, "credentials.json")
INSTRUMENTS_FILE = os.path.join(BROKER_DIR, "instruments.csv")
ACTION_PROPERTIES_FILE = os.path.join(STRATEGY_DIR, "properties.json")

LOGS_FOLDER = os.path.join(BASE_DIR, "Logs")
CSV_LOGS_FILE = os.path.join(LOGS_FOLDER, "order_log.csv")
SHORT_STRADDLE_ORDER_LOG_FILE = os.path.join(BASE_DIR, "short_straddle_orders.csv")

# CREATING CREDENTIAL FILE TEMPLATES
# ===========================================================================================
if not os.path.exists(BROKER_CREDENTIALS_FILE):
    broker_cred = {
        "name": "zerodha",
        "user_id": "",
        "password": "",
        "api_key": "",
        "api_secret": "",
        "totp_code": ""
    }
    with open(BROKER_CREDENTIALS_FILE, 'w') as file:
        json.dump(broker_cred, file)

# GLOBAL VARIABLES
# ===========================================================================================
SLEEP_TIME_BETWEEN_ATTEMPTS = 1   # Time (in sec) for which the process will sleep before retyring

MAX_BROKER_LOGIN_ATTEMPT_COUNT = 5  # Number of attempts made to login with the broker
MAX_FETCH_INSTRUMENT_FILE_ATTEMPT_COUNT = 3 # Number of attempts made to fetch and load the instruments file
MAX_ORDER_PLACEMENT_RETRIES = 5 # Number of attempts to place the order
MAX_ORDER_CANCELLATION_RETRIES = 5  # Number of attempts to cancel an order
HISTORICAL_DATA_FETCH_MAX_RETRY = 10    # Number of retries to fetch historical data

TICKER_RETRY_TIMEOUT = 5    # Time (in sec) till we will wait for ticker to start
DATA_UPDATE_TIME = 3    # Time after which live data is updated