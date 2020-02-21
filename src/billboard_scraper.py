import json, billboard
from pymongo import MongoClient
from datetime import datetime
import time

def run(date=None, verbose=1):
    collection = MongoClient()['billboard']['hot100']

    kwargs = {'name':'hot-100', 'max_retries':5, 'timeout':120}
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
         time.sleep(2)

def clean():
    """
    Cleans raw data from the billboard scraper.
    """
    db = MongoClient().billboard 
    hot100filtered = db['hot100filtered']
    hot100raw = db.hot100

    df = pd.DataFrame(hot100raw.find())\
        .drop(columns=['_id', '_max_retries', '_timeout', 'name', 'nextDate', 'title'])
    alltracks=[]
    for row in df.values:
        date = row[0]
        for track in row[1]:
            track['date'] = date
            alltracks.append(track)
    df = pd.DataFrame(alltracks)
    df = df.groupby(['artist', 'title'])\
        .agg({'date':min, 'peakPos':max, 'weeks':max}, axis='columns').reset_index()
    for i in range(df.shape[0]):
        hot100filtered.insert_one(df.iloc[i].to_dict())


