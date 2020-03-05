import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

import pandas as pd
from pymongo import MongoClient
import time, sys, string
from urllib.parse import urlparse, parse_qs

from pymongo.errors import BulkWriteError, DuplicateKeyError


class Spotify_Scraper:
    """
    Scraper using spotipy to gather information from the Spotify API.

    Args:
        sleeptime (float): seconds to wait between queries. Default 0.5
    """

    def __init__(self, sleeptime=0.5):

        # get Spotify API tokens
        self.sleeptime = sleeptime
        with open("data/spotify.auth", "r") as f:
            client_id = f.readline().strip()
            client_secret = f.readline().strip()

        client_credentials_manager = SpotifyClientCredentials(
            client_id=client_id, client_secret=client_secret
        )
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        print("Connected to Spotify")

        self.db = MongoClient().billboard
        print("Connected to MongoDB")

    def populate_bb_scrapables(self):
        """
        Identifies and populates the tracks to scrape from the 
        billboard.hot100filtered collection.

        Returns: None. Saves track information to an object state.
        """
        to_scrape = [
            track for track in self.db.hot100filtered.find({"_id": {"$exists": "true"}})
        ]
        print(f"Billboard tracks:    {len(to_scrape)}")

        spot_ids = {
            track["_id"]
            for track in self.db["spotify"].find(
                {"_id": {"$exists": "true"}}, {"_id": "true"}
            )
        }

        self.to_scrape = []
        for track in to_scrape:
            if (track["_id"] not in spot_ids) & (track["date"] > "2000"):
                self.to_scrape.append(track)
        print(f"Remaining to scrape: {len(self.to_scrape)}")

    def add_full_bb_albums(self, verbose=1):
        """
        Scrapes full album data from Spotify given that billboard.spotify is populated.

        Args: 
            verbose (int): how much to print. Default 1

        Returns: None, but saves albums to billboard.spotify_albums 
        """
        to_scrape = [track["metadata"]["album"] for track in self.db.spotify.find()]
        num_albums = len(to_scrape)
        duplicates = 0
        for i, album in enumerate(to_scrape):
            album_id = album["id"]
            full_album = self.sp.album(album_id)
            next_tracks = full_album["tracks"]["next"]
            while next_tracks:
                offsetn = int(parse_qs(urlparse(next_tracks).query)["offset"][0])
                remaining_tracks = self.sp.album_tracks(album_id, offset=offsetn)
                full_album["tracks"]["items"].extend(remaining_tracks["items"])
                next_tracks = remaining_tracks["next"]
            full_album["_id"] = full_album.pop("id")
            try:
                self.db["spotify_albums"].insert_one(full_album)
            except DuplicateKeyError:
                duplicates += 1
            if verbose:
                print(f"Full Album {i}/{num_albums} scraped")
        print(f"Duplicates encountered: {duplicates}")

    def populate_nillboard_scrapables(self, verbose=1):
        """
        Identifies and saves the nillboard tracks to scrape from the 
        non-billboard tracks in the scraped albums. 

        Args: 
            verbose (int): how much to print. Default 1

        Returns: None, but changes internal state.
        """
        track_ids = []
        for album in self.db.spotify_albums.find():
            track_ids.extend([track["id"] for track in album["tracks"]["items"]])
        billboard_ids = {track["metadata"]["id"] for track in self.db.spotify.find()}
        self.to_scrape = list(set(track_ids) - billboard_ids)

        if verbose:
            print(f"Nillboard tracks to scrape: {len(self.to_scrape)}")

    def scrape_nillboard_tracks(self, verbose=1):
        """
        Scrapes tracks from Spotify given that internal state has been populated.
        
        Args:
            verbose (int): how much to print. Default 1

        Returns: None, but saves track information to billboard.spotify_nillboard
        """
        end = len(self.to_scrape)
        for i in range(50, end + 1, 50):
            bundle = self.to_scrape[i - 50 : i]
            self.scrape_tracks_by_id_bundle(bundle)
            if verbose:
                print(f"Tracks scraped: {i}")
        if i < end:
            bundle = self.to_scrape[i:end]
            self._scrape_nillboard_tracks_by_id_bundle(bundle)

    def _scrape_nillboard_tracks_by_id_bundle(self, id_bundle):
        """
        Helper method for scrape_nillboard_tracks that scrapes tracks by a 
        bundle of ids the same size as the Spotify max pooled requests. 

        Args:
            id_bundle (list of str): track ids to scrape

        Returns: None, but saves track information to billboard.spotify_nillboard
        """
        metadata = self.sp.tracks(id_bundle)
        audio_features = self.sp.audio_features(id_bundle)
        af = dict()
        for a in audio_features:
            if a:
                af[a["id"]] = a
        tracks = []
        for m in metadata["tracks"]:
            track = dict()
            track_id = m["id"]
            track["metadata"] = m
            if track_id in af:
                track["audio_features"] = af[track_id]
            track["_id"] = track_id
            tracks.append(track)
        self.db["spotify_nillboard"].insert_many(tracks)

    def get_spotify_track(self, artist, trackname):
        """
        Searches Spotify with a artist name and track name and returns the result.

        Args:
            artist (str): Name of the artist.
            trackname (str): Name of the track.

        Returns: dict of track information or None if no results found.
        """
        artist = stripFeat(artist)
        result = self.sp.search(f"artist:{artist} track:{trackname}")

        # if no result, strip punctuation and try again
        if result["tracks"]["total"] == 0:
            trackname = trackname.translate(str.maketrans("", "", string.punctuation))
            result = self.sp.search(f"artist:{artist} track:{trackname}")
            if result["tracks"]["total"] == 0:
                return None

        return result["tracks"]["items"][0]

    def scrape_all(self, hook, hkwargs, verbose=1):
        """
        Scrapes all tracks given a populated internal state with artist and 
        track title information.

        Args:
            hook (function): Used to process the list of dicts before
                inserting the documents into MongoDB.
            hkwargs (dict): Keyword arguments for the hook.
            verbose (int): how much to print 

        Returns: None, but saves data to billboard.spotify collection.
        """
        i = 0
        num_tracks = len(self.to_scrape)
        while i < num_tracks:
            prev_i = i
            tracks_arr = []
            err_arr = []
            while (i - prev_i < 50) & (i < num_tracks):
                row = self.to_scrape[i]
                _id = row["_id"]
                trackdata = self.get_spotify_track(row["artist"], row["title"])
                if trackdata == None:
                    err_arr.append({"_id": _id, "msg": "No Spotify data"})
                else:
                    tracks_arr.append({"_id": _id, "metadata": trackdata})
                i += 1
                if verbose == 2:
                    print(i)
                time.sleep(self.sleeptime)
            URIlist = list(map(lambda x: x["metadata"]["uri"], tracks_arr))
            if verbose:
                print(f"__index: {i}")
                print(f"Scraped: {len(tracks_arr)}")
                print(f"Errors:  {len(err_arr)}")
            af_arr = self.sp.audio_features(URIlist)
            if hook:
                tracks_arr = hook(tracks_arr, **hkwargs)
            self.insert_to_mongo(tracks_arr, af_arr, err_arr)

    def insert_to_mongo(self, tracks_arr, af_arr, err_arr):
        """
        Inserts track data to MongoDB.

        Args: 
            tracks_arr (list of dicts): array of metadata for tracks
            af_arr (list of dicts): array of audio features for tracks
            err_arr (list of dicts): array of error documents

        Returns: None, but saves track data to billboard.spotify and errors 
        to billboard.spotify_errlog.
        """
        af, af_arr = af_arr.copy(), dict()
        for features in af:
            if features != None:
                af_arr[features["uri"]] = features
        for track in tracks_arr:
            URI = track["metadata"]["uri"]
            track_af = af_arr.get(URI, None)
            if track_af != None:
                track["audio_features"] = af_arr[URI]
            else:
                err_arr.append(
                    {"_id": track["_id"], "msg": "No audio features returned"}
                )
                print(f'No audio features returned for track: {track["_id"]}')
        try:
            for track in tracks_arr:
                try:
                    self.db.spotify.insert_one(track)
                except DuplicateKeyError:
                    print("DuplicateKeyError: track already in audio_features")
        except BulkWriteError as bwe:
            print(bwe.details)

        try:
            for e in err_arr:
                try:
                    self.db["spotify_errlog"].insert_one(e)
                except DuplicateKeyError:
                    print("Duplicate Key Error adding to spotify_errlog")
        except BulkWriteError as bwe:
            print(bwe.details)
        except DuplicateKeyError:
            print("Duplicate Key Error adding to audio_errlog")


def insert_kv(arr, k, v):
    """
    Inserts a key-value pair into all dict elements of a array.

    Args:
        arr (list of dicts): array with dicts to insert the k-v pair
        k (hashable): key to insert
        v (object): value to insert

    Returns: altered array of dicts
    """
    for item in arr:
        item[k] = v
    return arr


def stripFeat(s):
    """
    Strips featured artists from the artist title according to 
    Billboard Hot 100 formatting.

    Args:
        s (str): artist name

    Returns: (str) altered artist name 
    """
    if " Featuring" in s:
        return s[: s.index(" Featuring")]
    elif " x " in s:
        return s[: s.index(" x ")]
    else:
        return s


if __name__ == "__main__":

    s = Spotify_Scraper(0.5)
    s.populate_nillboard_scrapables()
    s.scrape_tracks_by_ids()

    # s.add_full_bb_albums()

    # hook_kwargs = {'k':'on_billboard', 'v':True}
    # s.scrape_all(insert_kv, hook_kwargs, verbose=1)
