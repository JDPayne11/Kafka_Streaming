from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage
from typing import List

# Note: Multiple subscriptions can be added to the array 
# For example, if you want to subscribe to AAPL and META,
# you can do so by adding "T.META" to the subscriptions array. ["T.AAPL", "T.META"]
# If you want to subscribe to all tickers, place an asterisk in place of the symbol. ["T.*"]
ws = WebSocketClient(api_key=<API_KEY>, subscriptions=["T.AAPL"])