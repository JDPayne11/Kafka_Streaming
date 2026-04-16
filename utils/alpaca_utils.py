import os
from dotenv import load_dotenv
from alpaca.data.live import StockDataStream, OptionDataStream, CryptoDataStream
from alpaca.data.live.websocket import DataStream
from utils.kafka_utils import acked
from confluent_kafka import Producer


load_dotenv()
ALPACA_API_KEY = os.environ["ALPACA_API_KEY"]
ALPACA_API_SECRET = os.environ["ALPACA_API_SECRET"]

STREAM_MAP = {
    'StockDataStream': StockDataStream,
    'OptionDataStream': OptionDataStream,
    'CryptoDataStream': CryptoDataStream,
}



def make_handler(producer: Producer, topic: str):
    async def handler(data):
        producer.produce(
            topic=topic,
            key=data.symbol,
            value=data.json(),
            callback=acked
        )
        producer.poll(0)
    return handler


def stream_starter(
        topic:str,
        config:dict,
        producer:Producer,
        api_key:str = ALPACA_API_KEY, 
        api_secret:str = ALPACA_API_SECRET,
        test_mode:bool = False
) -> DataStream:
    
    stream_type = config['stream_type']
    stream_class = STREAM_MAP.get(stream_type)
    if stream_class is None:
        raise ValueError(f"Invalid stream type {stream_type}")
    tickers = config['tickers']
    subscription = config['subscription']

    if test_mode:
        test_url = 'wss://stream.data.alpaca.markets/v2/test'  
        tickers = ('FAKEPACA',)
    else: 
        test_url = None

    stream = stream_class(api_key, api_secret, url_override=test_url)
    handler = make_handler(producer, topic)
    getattr(stream, subscription)(handler, *tickers)


    stream.run()
    return stream







