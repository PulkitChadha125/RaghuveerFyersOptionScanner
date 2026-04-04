from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import webbrowser
from datetime import datetime, timedelta, date
from time import sleep
import os
import pyotp
import requests
import json
import math
import pytz
from urllib.parse import parse_qs, urlparse
import warnings
import pandas as pd
access_token=None
fyers=None
shared_data = {}
shared_data_2 = {}
# Lock to ensure thread-safe access to the shared data
def apiactivation(client_id, redirect_uri, response_type, state, secret_key, grant_type):
    from fyers_apiv3 import fyersModel
    import webbrowser

    appSession = fyersModel.SessionModel(
        client_id=client_id,
        redirect_uri=redirect_uri,
        response_type=response_type,
        state=state,
        secret_key=secret_key,
        grant_type=grant_type
    )

    try:
        generateTokenUrl = appSession.generate_authcode()
        print("generateTokenUrl:", generateTokenUrl)

        # If it's a full URL, open browser (manual login)
        if generateTokenUrl.startswith("https://"):
            print("Opening browser for manual login...")
            webbrowser.open(generateTokenUrl, new=1)
            return generateTokenUrl

        # Else, assume it's an auth code directly
        elif isinstance(generateTokenUrl, dict) and "data" in generateTokenUrl and "auth" in generateTokenUrl["data"]:
            print("Auth code obtained directly:", generateTokenUrl["data"]["auth"])
            return generateTokenUrl["data"]["auth"]

        else:
            print("Unexpected response format:", generateTokenUrl)
            return None

    except Exception as e:
        print("Error during auth code generation:", e)
        return None


def automated_login(client_id,secret_key,FY_ID,TOTP_KEY,PIN,redirect_uri):

    pd.set_option('display.max_columns', None)
    warnings.filterwarnings('ignore')

    import base64


    def getEncodedString(string):
        string = str(string)
        base64_bytes = base64.b64encode(string.encode("ascii"))
        return base64_bytes.decode("ascii")

    global fyers,access_token

    URL_SEND_LOGIN_OTP = "https://api-t2.fyers.in/vagator/v2/send_login_otp_v2"
    response = requests.post(url=URL_SEND_LOGIN_OTP, json={"fy_id": getEncodedString(FY_ID), "app_id": "2"})
    print("Status code:", response.status_code)
    print("Raw text:", response.text)
    res = response.json()

    if datetime.now().second % 30 > 27: sleep(5)
    URL_VERIFY_OTP = "https://api-t2.fyers.in/vagator/v2/verify_otp"
    res2 = requests.post(url=URL_VERIFY_OTP,
                         json={"request_key": res["request_key"], "otp": pyotp.TOTP(TOTP_KEY).now()}).json()
    print(res2)

    ses = requests.Session()
    URL_VERIFY_OTP2 = "https://api-t2.fyers.in/vagator/v2/verify_pin_v2"
    payload2 = {"request_key": res2["request_key"], "identity_type": "pin", "identifier": getEncodedString(PIN)}
    res3 = ses.post(url=URL_VERIFY_OTP2, json=payload2).json()
    print("res3: ",res3)

    ses.headers.update({
        'authorization': f"Bearer {res3['data']['access_token']}"
    })

    TOKENURL = "https://api-t1.fyers.in/api/v3/token"
    payload3 = {"fyers_id": FY_ID,
                "app_id": client_id[:-4],
                "redirect_uri": redirect_uri,
                "appType": "100", "code_challenge": "",
                "state": "None", "scope": "", "nonce": "", "response_type": "code", "create_cookie": True}

    res3 = ses.post(url=TOKENURL, json=payload3).json()
    print("res3: ",res3)
    url = res3['Url']
    parsed = urlparse(url)
    auth_code = parse_qs(parsed.query)['auth_code'][0]
    grant_type = "authorization_code"

    response_type = "code"

    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type=response_type,
        grant_type=grant_type
    )
    session.set_token(auth_code)
    response = session.generate_token()
    access_token = response['access_token']
    print("access_token: ",access_token)
    fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path=os.getcwd())
    print(fyers.get_profile())

def get_ltp(SYMBOL):
    global fyers
    data={"symbols":f"{SYMBOL}"}
    res=fyers.quotes(data)
    if 'd' in res and len(res['d']) > 0:
        lp = res['d'][0]['v']['lp']
        return lp

    else:
        print("Last Price (lp) not found in the response.")




def get_position():
    global fyers
      ## This will provide all the trade related information
    res=fyers.positions()
    return res

def get_orderbook():
    global fyers
    res = fyers.orderbook()
    return res
      ## This will provide the user with all the order realted information

def get_tradebook():
    global fyers
    res = fyers.tradebook()
    return res


def fetchOHLC_Scanner(symbol):
    dat =str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(5)).date())
    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat ,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)
    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)
    df['date']=df['date'].apply(pd.Timestamp,unit='s',tzinfo=pytz.timezone('Asia/Kolkata'))
    return df.tail(5)

def fetchOHLC_Weekly(symbol):
    from datetime import datetime, timedelta
    import pandas as pd
    import numpy as np

    # Extended range for full candle history
    today = datetime.now()
    dat = str((today + timedelta(days=1)).date())
    dat1 = str((today - timedelta(days=160)).date())

    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat,
        "cont_flag": "1"
    }

    response = fyers.history(data=data)

    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)

    # Convert timestamp to datetime in IST
    df['date'] = pd.to_datetime(df['date'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
    df.set_index('date', inplace=True)


    # ============ Weekly OHLC ============
    df_weekly = df.resample('W-FRI').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    # ============ Monthly OHLC with actual last available dates ============

    df['year'] = df.index.year
    df['month'] = df.index.month

    # Group by (year, month)
    grouped = df.groupby(['year', 'month'])

    records = []
    index_dates = []

    for (y, m), group in grouped:
        open_price = group['open'].iloc[0]
        high_price = group['high'].max()
        low_price = group['low'].min()
        close_price = group['close'].iloc[-1]
        volume_sum = group['volume'].sum()

        # Use the actual last trading day in the group as index
        last_date = group.index[-1]
        index_dates.append(last_date)

        records.append([open_price, high_price, low_price, close_price, volume_sum])

    df_monthly = pd.DataFrame(records, columns=['open', 'high', 'low', 'close', 'volume'], index=index_dates)

    # Ensure index is sorted
    df_monthly.sort_index(inplace=True)



    return df_weekly, df_monthly





# def fetchOHLC_Weekly(symbol):
#     # Approx 140 days for 20 weeks of daily data
#     dat = str(datetime.now().date())
#     dat1 = str((datetime.now() - timedelta(days=140)).date())

#     data = {
#         "symbol": symbol,
#         "resolution": "1D",
#         "date_format": "1",
#         "range_from": dat1,
#         "range_to": dat,
#         "cont_flag": "1"
#     }

#     response = fyers.history(data=data)
#     # print("response weekly:", response)

#     cl = ['date', 'open', 'high', 'low', 'close', 'volume']
#     df = pd.DataFrame(response['candles'], columns=cl)

#     # Convert Unix timestamp to datetime in IST
#     df['date'] = pd.to_datetime(df['date'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
#     df.set_index('date', inplace=True)

#     # Resample to weekly candles, week ending on Friday
#     df_weekly = df.resample('W-FRI').agg({
#         'open': 'first',
#         'high': 'max',
#         'low': 'min',
#         'close': 'last',
#         'volume': 'sum'
#     })

#     # Drop incomplete weeks
#     df_weekly.dropna(inplace=True)

#     return df_weekly  # Return last 20 weeks

def fetchOHLC(symbol,tf):
    print("symbol: ",symbol)
    dat =str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(17)).date())
    data = {
        "symbol": symbol,
        "resolution":str(tf),
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)
    # print("response: ",response)
    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)
    df['date']=df['date'].apply(pd.Timestamp,unit='s',tzinfo=pytz.timezone('Asia/Kolkata'))
    return df


def fetchOHLC_get_selected_price(symbol, date):

    print("option symbol :",symbol)
    print("option symbol date :", date)
    dat = str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(25)).date())
    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)
    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)
    df['date'] = pd.to_datetime(df['date'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata').dt.date
    target_date = pd.to_datetime(date).date()
    matching_row = df[df['date'] == target_date]
    if matching_row.empty:
        return 0
    else:
        close_price = matching_row.iloc[0]['close']
        return close_price
    



def fyres_websocket(symbollist):
    from fyers_apiv3.FyersWebsocket import data_ws
    global access_token

    def onmessage(message):
        """
        Callback function to handle incoming messages from the FyersDataSocket WebSocket.

        Parameters:
            message (dict): The received message from the WebSocket.

        """
        # print("Response:", message)
        try:
            if 'symbol' in message:
                symbol = message.get('symbol')
                ltp = message.get('ltp')
                last_traded_qty = message.get('last_traded_qty')
                vol_traded_today = message.get('vol_traded_today')
                ts = message.get('exch_feed_time') or message.get('last_traded_time')
                shared_data[symbol] = {
                    'ltp': ltp,
                    'last_traded_qty': last_traded_qty,
                    'vol_traded_today': vol_traded_today,
                    'timestamp': ts
                }
        except Exception as e:
            print("onmessage parse error:", e)




    def onerror(message):
        """
        Callback function to handle WebSocket errors.

        Parameters:
            message (dict): The error message received from the WebSocket.


        """
        print("Error:", message)


    def onclose(message):
        """
        Callback function to handle WebSocket connection close events.
        """
        print("Connection closed:", message)


    def onopen():
        """
        Callback function to subscribe to data type and symbols upon WebSocket connection.

        """
        # Specify the data type and symbols you want to subscribe to
        data_type = "SymbolUpdate"

        # Subscribe to the specified symbols and data type
        symbols = symbollist
        # ['NSE:LTIM24JULFUT', 'NSE:BHARTIARTL24JULFUT']
        fyers.subscribe(symbols=symbols, data_type=data_type)

        # Keep the socket running to receive real-time data
        fyers.keep_running()


    # Replace the sample access token with your actual access token obtained from Fyers
    # access_token = "XC4XXXXXXM-100:eXXXXXXXXXXXXfZNSBoLo"

    # Create a FyersDataSocket instance with the provided parameters
    fyers = data_ws.FyersDataSocket(
        access_token=access_token,  # Access token in the format "appid:accesstoken"
        log_path="",  # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=False,  # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,  # Save response in a log file instead of printing it.
        reconnect=True,  # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=onopen,  # Callback function to subscribe to data upon connection.
        on_close=onclose,  # Callback function to handle WebSocket connection close events.
        on_error=onerror,  # Callback function to handle WebSocket errors.
        on_message=onmessage  # Callback function to handle incoming messages from the WebSocket.
    )

    # Establish a connection to the Fyers WebSocket
    fyers.connect()

def fyres_quote(symbol):
    data = {
        "symbols": f"{symbol}"
    }

    response = fyers.quotes(data=data)
    return response





def fyres_websocket_option(symbollist):
    from fyers_apiv3.FyersWebsocket import data_ws
    global access_token

    def onmessage(message):
        """
        Callback function to handle incoming messages from the FyersDataSocket WebSocket.

        Parameters:
            message (dict): The received message from the WebSocket.

        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{timestamp} - {message}\n")
        if 'symbol' in message and 'ltp' in message:
            shared_data_2[message['symbol']] = message['ltp']




    def onerror(message):
        """
        Callback function to handle WebSocket errors.

        Parameters:
            message (dict): The error message received from the WebSocket.


        """
        print("Error:", message)


    def onclose(message):
        """
        Callback function to handle WebSocket connection close events.
        """
        print("Connection closed:", message)


    def onopen():
        """
        Callback function to subscribe to data type and symbols upon WebSocket connection.

        """
        # Specify the data type and symbols you want to subscribe to
        data_type = "SymbolUpdate"

        # Subscribe to the specified symbols and data type
        symbols = symbollist
        # ['NSE:LTIM24JULFUT', 'NSE:BHARTIARTL24JULFUT']
        fyers.subscribe(symbols=symbols, data_type=data_type)

        # Keep the socket running to receive real-time data
        fyers.keep_running()


    # Replace the sample access token with your actual access token obtained from Fyers
    # access_token = "XC4XXXXXXM-100:eXXXXXXXXXXXXfZNSBoLo"

    # Create a FyersDataSocket instance with the provided parameters
    fyers = data_ws.FyersDataSocket(
        access_token=access_token,  # Access token in the format "appid:accesstoken"
        log_path="",  # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=True,  # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,  # Save response in a log file instead of printing it.
        reconnect=True,  # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=onopen,  # Callback function to subscribe to data upon connection.
        on_close=onclose,  # Callback function to handle WebSocket connection close events.
        on_error=onerror,  # Callback function to handle WebSocket errors.
        on_message=onmessage  # Callback function to handle incoming messages from the WebSocket.
    )

    # Establish a connection to the Fyers WebSocket
    fyers.connect()



def place_order(symbol,quantity,type,side,price):
    # Set quantity to 1 by default if not provided
    if quantity is None or quantity == 0:
        quantity = 1
    quantity = int(quantity)
    price = float(price)
    
    # Keep type as integer (1=Limit, 2=Market)
    order_type = int(type)
    
    # Keep side as integer (1=Buy, -1=Sell)
    order_side = int(side)
    
    print("quantity: ",quantity)
    print("price: ",price)
    print("type: ",order_type)
    print("side: ",order_side)
    
    # For market orders (type=2), set limitPrice to 0
    limit_price = 0 if order_type == 2 else price
    
    # Use the exact field names and data types from Fyers API documentation
    data = {
        "symbol": symbol,
        "qty": quantity,
        "type": order_type,
        "side": order_side,
        "productType": "INTRADAY",
        "limitPrice": limit_price,
        "stopPrice": 0,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
        "stopLoss": 0,
        "takeProfit": 0,
        "orderTag": "tag1"
    }
    
    print("Order data: ", data)
    response = fyers.place_order(data=data)
    print("response: ",response)
    return response

