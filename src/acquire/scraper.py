from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import logging as log
import os
from util.logger import setup_logging

logger = setup_logging()

class Scraper:

    def __init__(self):
        self.soup: bs = None
        self.keys:list = []
        self.constituents:list = []

    def wikipedia_sp500_scrape(
            self, 
            url:str = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    )->None:
        
        self.soup = bs(requests.get(url).text)

        for x in self.soup.find('table', {"id":"constituents"}).find_all('tr'):
            if x.find_all('th'):
                for y in x.find_all('th'):
                    self.keys.append(y.text.replace("\n", ""))
            elif x.find_all('td'):
                temp_obj = {}
                for num, y in enumerate(x.find_all('td')):
                    temp_obj[self.keys[num]] = y.text.replace('\n', '')
                    self.constituents.append(temp_obj)

    def make_dict(self)->dict:

        constituent_dict = {}

        for x in self.constituents[1:]:
            try:
                temp_dict = x.copy()
                symbol = temp_dict['Symbol']
                constituent_dict[symbol] = temp_dict
            except:
                print(x)

        return constituent_dict
    
    def make_pd(self)->pd.DataFrame:     
        return pd.DataFrame([x for key, x in self.make_dict().items()]) 
    
    @staticmethod
    def save_df(
        df, 
        path, 
        format='parquet'
    ):
        assert os.path.isfile(path)

        if format=='parquet':
            df.to_parquet(path)
        #TODO: Add functionality for other formats, feather, SQL db
        else:
            logger.error("Can only save to parquet, adding support for other formats later")
