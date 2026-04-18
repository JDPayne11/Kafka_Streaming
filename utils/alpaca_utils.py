import os
import logging
from dotenv import load_dotenv
from alpaca.data.live import StockDataStream, OptionDataStream, CryptoDataStream
from alpaca.data.live.websocket import DataStream
from utils.kafka_utils import acked, create_topic
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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
        data_stream: str,
        config: dict,
        producer:Producer,
        admin_client: AdminClient,
        created_topics: list[str],
        api_key:str = ALPACA_API_KEY, 
        api_secret:str = ALPACA_API_SECRET,
        test_mode:bool = False
) -> DataStream:
    
    logging.debug("Test mode: %s", test_mode)
    if test_mode:
        test_url = 'wss://stream.data.alpaca.markets/v2/test'  
    else: 
        test_url = None

    # Retrive and create data stream class
    stream_class = STREAM_MAP.get(data_stream)
    if stream_class is None:
        raise ValueError(f"Invalid stream type {data_stream}")
    
    logging.debug("Creating data stream...")
    stream = stream_class(api_key, api_secret, url_override=test_url)
    
    # iterate through subscriptions
    for topic, settings in config.items():
        if settings['active']:
            create_topic(admin_client, topic)
            created_topics.append(topic)
            if test_mode:
                tickers = ('FAKEPACA',)
            else:
                tickers = settings['tickers']

            subscription = settings['subscription']
            logging.debug("Making handler...")
            handler = make_handler(producer, topic)
            logging.info("Created handler %s", topic)
            # This sets up the subscription in the format: stream.subscribe_trades(handler, *tickers)
            logging.debug("Setting subscriptions...")
            getattr(stream, subscription)(handler, *tickers)

    logging.debug("Starting stream...")
    stream.run()

    return stream







