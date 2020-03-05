from lyricsgenius import Genius
from pymongo import MongoClient
import pandas as pd
import numpy as np

from io import StringIO
import sys, time
from functools import reduce
from operator import add

import traceback as tb
from requests.exceptions import ReadTimeout
from pymongo.errors import DuplicateKeyError


class Scraper:
    """
    Scraper for genius data. 

    Args: 
        genius_auth_path (str): filepath to the authorization text file with
            genius API client access token. Default is 'data/genius.auth'
        minsleep (float): minimum time to sleep between requests to the 
            genius API. Default is 0.5
    """

    def __init__(self, genius_auth_path="data/genius.auth", minsleep=0.5):

        # gets client access token
        with open("data/genius.auth", "r") as file:
            client_access_token = file.read().strip()

        self.minsleep = minsleep
        self.api = Genius(client_access_token, remove_section_headers=True)
        self.lyrics = MongoClient().billboard["lyrics"]
        self.errlog = MongoClient().billboard["lyrics_errlog"]
        print("Initialized")

    def populate_billboard_scrapables(self):
        """
        Identifies billboard tracks to scrape from the spotify collection.
        Returns: None. sets self.df
        """
        results = MongoClient().billboard.spotify.find()
        self.df = pd.DataFrame(
            data=map(
                lambda r: (
                    r["metadata"]["id"],
                    r["metadata"]["artists"][0]["name"],
                    r["metadata"]["name"],
                ),
                results,
            ),
            columns=["track_id", "artist_name", "title"],
        )
        print(f"Tracks identified to scrape lyrics: {self.df.shape[0]}")

    def populate_nillboard_scrapables(self):
        """
        Populates tracks to scraped that are not on the billboard
        Returns: None. Sets internal state as self.df
        """

        # initialize db connection
        db = MongoClient().billboard

        # get the track_ids that have already been scraped.
        scraped_ids = [r["_id"] for r in db.lyrics.find()] + [
            r["metadata"]["id"] for r in db.spotify.find()
        ]
        print("scraped ids", len(scraped_ids))

        # get the entries that have not yet been scraped
        tracks_cursor = db.spotify_nillboard.find({"_id": {"$nin": scraped_ids}})

        # unpack the db response cursor
        data = map(
            lambda r: [
                r["_id"],
                r["metadata"]["artists"][0]["name"],
                r["metadata"]["name"],
            ],
            tracks_cursor,
        )

        # set internal dataframe to be scraped
        self.df = pd.DataFrame(data, columns=["track_id", "artist_name", "title"])
        print(f"Tracks identified to scrape lyrics: {self.df.shape[0]}")

    def scrape_df_segment_to_db(self, scraperange, verbose=1):
        """
        Scrapes an index range from self.df to the database.

        Args:
            scraperange (iterable): iterable of indices to scrape from self.df
                to the db
            verbose (int): verbosity level. Higher verbosity, more prints.

        Returns: None. Puts genius data in billboard.lyrics and errors in 
            billboard.lyrics_errlog when needed.
        """

        for i in scraperange:
            row = self.df.iloc[i]
            try:
                self.scrape_song_to_db(
                    row["artist_name"], row["title"], row["track_id"]
                )

            # record error and continue
            except TypeError as e:
                self.record_error(row["track_id"], "TypeError")

            # track has already been scraped to the db - print and continue
            except DuplicateKeyError:
                if verbose:
                    print(f"Duplicate skipped on index {i}")

            if verbose > 1:
                print(i)

    def scrape_song_to_db(self, artist, title, track_id):
        """
        Scrapes a single track to the database. 

        Args: 
            artist (str): the artist name
            title (str): the title of the track
            track_id (str): the id of the track to be used as the mongodb _id 

        Returns: None. Adds a track to the lyrics collection, or lyrics_errlog 
        if needed. 
        """

        # remove featured artist names
        artist = stripFeat(artist)

        try:
            # record stout from lyricsgenius call because it catches errors and prints
            with Capturing() as output:
                songdata = self.api.search_song(title, artist)

        # for the few errors that have been raised
        except ReadTimeout:
            self.api.sleep_time += 3
            print(f"sleep time increased to {self.api.sleep_time}")
            self.record_error(track_id, "ReadTimeout")
            self.scrape_song_to_db(artist, title, track_id)
            return

        # take sleep time slowly back to minimum
        if self.api.sleep_time > self.minsleep:
            self.api.sleep_time -= 0.25
            print(f"sleep time decreased to {self.api.sleep_time}")

        # search successful
        if songdata != None:
            self.record_lyrics_result(track_id, songdata)

        # handle (record & retry) Timeout error
        elif output[1].startswith("Timeout"):
            self.api.sleep_time += 3
            self.record_error(track_id, "Timeout")
            self.scrape_song_to_db(artist, title, track_id)
            return

        # record error: not in genius db
        elif output[1].startswith("No results"):
            self.record_error(track_id, "no_results")

        # record error: song without lyrics
        elif output[1] == "Specified song does not contain lyrics. Rejecting.":
            self.record_error(track_id, "lacks_lyrics")

        # record error: URL issue
        elif (
            output[1]
            == "Specified song does not have a valid URL with lyrics. Rejecting."
        ):
            self.record_error(track_id, "invalid_url")

    def record_lyrics_result(self, track_id, songdata):
        """
        Inserts a track's lyrics to the lyrics collection.

        Args: 
            track_id (str): spotify track id to be the mongodb _id
            songdata (dict): contains track data in keys 'artist', 'title', and 'lyrics'

        Returns: None. A song is inserted into the lyrics collection.
        """
        self.lyrics.insert_one(
            {
                "_id": track_id,
                "response_artist": songdata.artist,
                "response_title": songdata.title,
                "lyrics": songdata.lyrics,
            }
        )

    def record_error(self, track_id, errtype):
        """
        Inserts the record of an error into the errlog collection.

        Args:
            track_id (str): id of the track this error occurred on
            errtype (str): type of error that occurred.

        Returns: None. Inserts 1 record into the errlog collection
        """
        self.errlog.insert_one({"track": track_id, "type": errtype})

    def record_error_verbose(self, track_id, errmsg):
        """
        Inserts a verbose error into the errlog collection.

        Args:   
            track_id (str): id of the track this error occurred on
            errmsg (str): error message to record

        Returns: None. An error of type 'verbose' is inserted into the errlog collection.
        """
        self.errlog.insert_one(
            {"track": track_id, "type": "verbose", "message": errmsg}
        )


def stripFeat(s):
    """
    Removes the names of featured artists.

    Params:
        s (str): the full, uncleaned, artist name field

    Returns: the name of the first artist.
    """
    if " Featuring" in s:
        return s[: s.index(" Featuring")]
    elif " x " in s:
        return s[: s.index(" x ")]
    else:
        return s


class Capturing(list):
    """
    Captures stdout as a list. Needed for error handling with lyricsgenius 
    because those errors are simpy printed. Meant to be used in with as blocks.
    """

    def __enter__(self):
        """
        Starts redirecting stdout. 

        Args: None

        Returns: self 
        """
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        """
        Exits collecting stdout, replacing it with the default, and puts the 
        captured output in self.

        Args: 
            args - not used

        Returns: None, but this object is ready to use as a list of the
        output lines.
        """
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio  # free up some memory
        sys.stdout = self._stdout


if __name__ == "__main__":
    s = Scraper()
    s.populate_nillboard_scrapables()
    scraperange = range(0, s.df.shape[0])
    s.scrape_df_segment_to_db(scraperange, 2)
