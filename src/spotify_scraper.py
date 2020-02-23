import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo.errors import DuplicateKeyError
import pandas as pd
import sqlite3, time, sys

class Spotify_Scraper:
    def __init__(self, sleeptime=0.5):

        #get Spotify API tokens
        self.sleeptime = sleeptime
        with open('data/spotify.auth', 'r') as f:
            client_id = f.readline().strip()
            client_secret = f.readline().strip()

        client_credentials_manager = SpotifyClientCredentials(client_id=client_id, 
            client_secret=client_secret)
        self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        print('Connected to Spotify')
    
        self.db = MongoClient().billboard
        print('Connected to MongoDB')

        self.populate_scrapables()

    def populate_scrapables(self):
        to_scrape = [track for track in self.db.hot100filtered.find({'_id':{'$exists':'true'}})]
        print(f'Billboard tracks:    {len(to_scrape)}')
                
        spot_ids = {track['_id'] for track in self.db.['spotify'].find({'_id':{'$exists':'true'}}, {'_id':'true'})}
        
        self.to_scrape = []
        for track in to_scrape:
            if track['_id'] not in spot_ids:
                self.to_scrape.append(track)
        print(f'Remaining to scrape: {len(self.to_scrape)}')

    def get_spotify_URI(self, artist, trackname):
        result = self.sp.search(f'artist:{artist} track:{trackname}')
        if(result['tracks']['total'] == 0):
            return None
        else:
            return result['tracks']['items'][0]
    
        return self.sp.audio_features(URIlist)

    def scrape_all(self, verbose=1):

        i = 0
        num_tracks = len(self.to_scrape)
        while i < num_tracks:
            prev_i = i
            tracks_arr = []
            err_arr = []
            while (i-prev_i < 50) & (i < num_tracks):
                row = self.to_scrape[i]
                _id = row['_id']
                trackdata = self.get_spotify_URI(row['artist'], row['title'])
                if trackdata == None:
                    err_arr.append({'_id': _id, 'msg': 'No Spotify data'})
                else:
                    tracks_arr.append({'_id':_id, 'metadata': trackdata})
                i += 1
                if verbose == 2:
                    print(i)
                time.sleep(self.sleeptime)
            URIlist = list(map(lambda x: x['metadata']['uri'], tracks_arr))
            if verbose:
                print(f'__index: {i}')
                print(f'Scraped: {len(tracks_arr)}')
                print(f'Errors:  {len(err_arr)}')
            af_arr = self.sp.audio_features(URIlist)
            self.insert_to_mongo(tracks_arr, af_arr, err_arr)


    def insert_to_mongo(self, tracks_arr, af_arr, err_arr):
        af, af_arr = af_arr.copy(), dict() 
        for features in af:
            if features != None:
                af_arr[features['uri']] = features
        for track in tracks_arr:
            URI = track['metadata']['uri']
            track_af = af_arr.get(URI, None)
            if track_af != None:
                track['audio_features'] = af_arr[URI]
            else:
                err_arr.append({'_id':track['_id'], 'msg': 'No audio features returned'})
                print(f'No audio features returned for track: {track["_id"]}')
        try:
            for track in tracks_arr:
                try:
                    self.audio_features.insert_one(track)
                except DuplicateKeyError:
                    print('DuplicateKeyError: track already in audio_features')
            #self.audio_features.insert_many(tracks_arr, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)
        
        try:
            for e in err_arr:
                try:
                    self.audio_errlog.insert_one(e)
                except DuplicateKeyError:
                    print('Duplicate Key Error adding to audio_errlog')
            #self.audio_errlog.insert_many(err_arr)
        except BulkWriteError as bwe:
            print(bwe.details)
        except DuplicateKeyError:
            print('Duplicate Key Error adding to audio_errlog')


if __name__ == '__main__':
    s = Spotify_Scraper(0.5)
    
    #df_to_read = pd.read_csv('data/All_Billboard_MSD_Matches.csv', index_col=0)
    #s.df = df_to_read.rename(columns={'artist':'artist_name', 'track':'title', 'msdid':'track_id'})

    s.scrape_all(verbose=int(sys.argv[1]))

            

