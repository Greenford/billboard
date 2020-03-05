import json, billboard, time
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import numpy as np


def run(date=None, verbose=1):
    """
    Collects all Billboard Hot 100 data starting from the given date and
    iterating through previous dates.

    Args: 
        date: string in 'YYYY-MM-DD' format to start at.
        verbose: 0 or 1, with 1 meaning more status messages will be printed.

    Returns: nothing. Puts the gathered data in a mongo DB named billboard and 
        with collection name hot100.

    """

    # initialize DB connection
    collection = MongoClient()["billboard"]["hot100"]

    # initialize arguments and extend timeout
    kwargs = {"name": "hot-100", "max_retries": 5, "timeout": 120}
    if not date:
        date = billboard.ChartData(**kwargs).date

    while date:
        kwargs["date"] = date

        # get billboard data through billboard.py
        chart = billboard.ChartData(**kwargs)

        # prep chart data for mongo
        weekdata = json.loads(chart.json())
        weekdata["_id"] = weekdata["date"]
        weekdata["scraped"] = datetime.utcnow()

        # insert into mongo
        collection.insert_one(weekdata)

        if verbose:
            print(date)

        date = chart.previousDate
        time.sleep(2)


def clean():
    """
    Cleans raw data from the billboard scraper and puts it in 
    billboard.hot100filtered. Mostly simplifies the data to one row per 
    artist-track combination with the highest position on the chart and the 
    most number of weeks. 

    Args: None

    Returns: None. Does leave the cleaned data in billboard.hot100filtered.
    """

    # initialize db connections
    db = MongoClient().billboard
    hot100filtered = db["hot100filtered"]
    hot100raw = db.hot100

    # get raw data into a dataframe
    df = pd.DataFrame(hot100raw.find()).drop(
        columns=["_id", "_max_retries", "_timeout", "name", "nextDate", "title"]
    )

    # unpacks each chart into an array of date-artist-track-etc rows
    alltracks = []
    for row in df.values:
        date = row[0]
        for track in row[1]:
            track["date"] = date
            alltracks.append(track)

    df = pd.DataFrame(alltracks)

    # removes duplicate artist-track rows
    df = (
        df.groupby(["artist", "title"])
        .agg({"date": min, "peakPos": max, "weeks": max}, axis="columns")
        .reset_index()
    )

    # inserts rows to db
    for i in range(df.shape[0]):
        entry = df.iloc[i].to_dict()
        entry["peakPos"] = int(entry["peakPos"])
        entry["weeks"] = int(entry["weeks"])

        hot100filtered.insert_one(entry)
