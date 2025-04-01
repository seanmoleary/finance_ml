from dotenv import load_dotenv
import os
import time
import logging as log
from polygon import RESTClient
import datetime 
import pandas as pd
from util.logger import setup_logging

logger = setup_logging()

class Polygon:
    
    def __init__(self, path):
        self.api_key = self.get_api_key()
        self.client = RESTClient(api_key=self.api_key)
        self.ticker_url=self.make_ticker_path(self.api_key)
        self.tickers = self.make_ticker_list(self.load_df(path))
        self.ticker_df = self.get_data(self.tickers)

    @staticmethod
    def get_api_key(key='api_key'):
        load_dotenv()
        return os.getenv(key)
    
    @staticmethod
    def make_ticker_path(
        api_key,
        reference_base="https://api.polygon.io/v3/reference",
        active = False,
        limit=1000
    ):
        return f"{reference_base}/tickers?active={active}&apiKey={api_key}&limit={limit}"

    @staticmethod
    def load_df(path):
        return pd.read_parquet(path)
    
    @staticmethod
    def make_ticker_list(
            constituents_df,
            col='Symbol'
    ):
        return list(constituents_df['Symbol'])

    @staticmethod
    def make_kwargs():
        today = datetime.date.today()
        to = today.replace()
        from_ = to.replace(year=to.year-2)
        kwargs = {
            'timespan':'day',
            'multiplier':1,
            'from_':from_,
            'to':to
        }

    def get_aggs(
        self,
        ticker, 
        **kwargs
    ):
        aggs = self.client.get_aggs(
            ticker,
            **kwargs
        )
        temp_df = pd.DataFrame(aggs)
        temp_df['timestamp'] = temp_df.timestamp.apply(lambda x:datetime.date.fromtimestamp(x//1e3))
        temp_df['ticker'] = ticker
        temp_df.index = pd.MultiIndex.from_frame(temp_df[['ticker', 'timestamp']])
        return temp_df
    
    def get_data(
            self, 
            tickers, 
            **kwargs
        ):
        ticker_df=pd.DataFrame()
        while len(tickers)>0:
            try:
                ticker = tickers.pop()
                temp_df = self.get_aggs(ticker, **kwargs)
                ticker_df = pd.concat([ticker_df, temp_df])
                log.info(f"added ticker {ticker}")
            #TODO: ADD BREAK CONDITION FOR STATUS CODES 
            except:
                log.info(f"requeuing {ticker}")
                tickers.append(ticker)
                log.info("waiting for timeout")
                time.sleep(60)
        return ticker_df