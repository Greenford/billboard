import json, billboard
from pymongo import MongoClient
from datetime import datetime
import time

def run(date=None, verbose=1):
    collection = MongoClient()['billboard']['hot100']

    kwargs = {'name':'hot-100'}
    if not date:
        date = billboard.ChartData(**kwargs).date
    while date:
        kwargs['date'] = date
        chart = billboard.ChartData(**kwargs)
        weekdata = json.loads(chart.json())
        weekdata['_id'] = weekdata['date']
        weekdata['scraped'] = datetime.utcnow()
        collection.insert_one(weekdata)

        if verbose:
            print(date)

        date = chart.previousDate
        time.sleep(1)

def clean():
    """
    Cleans raw data from the billboard scraper.
    """
    pass

run()
