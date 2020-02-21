import json, billboard
from pymongo import MongoClient
from datetime import datetime
import time

def run(verbose=1):
    collection = MongoClient()['billboard']['hot100']

    kwargs = {'name':'hot-100'}
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
    pass

run()
