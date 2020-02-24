from lyricsgenius import Genius
from pymongo import MongoClient
from io import StringIO
import sys, sqlite3, time
import pandas as pd
import traceback as tb
from requests.exceptions import ReadTimeout
from pymongo.errors import DuplicateKeyError

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

class Scraper:

    def __init__(self, genius_auth_path='data/genius.auth', minsleep = 0.5):
        with open('data/genius.auth', 'r') as file:
            client_access_token = file.read().strip()
        self.minsleep = minsleep
        self.api = Genius(client_access_token, 
                          remove_section_headers=True)
        self.lyrics = MongoClient().billboard['lyrics']
        self.errlog = MongoClient().billboard['lyrics_errlog']

        #conn = sqlite3.connect('/mnt/snap/AdditionalFiles/track_metadata.db')
        #q = '''SELECT track_id, title, artist_name, year FROM songs 
        #       WHERE year >= 1958 ORDER BY year DESC;'''
        #df = pd.read_sql_query(q, conn)
        print('Initialized')


    def populate_billboard_scrapables(self):
        results = MongoClient().billboard.spotify.find()
        self.df = pd.DataFrame(data=map(lambda r: (r['metadata']['id'], 
            r['metadata']['artists'][0]['name'],
            r['metadata']['name']), results), 
            columns=['track_id', 'artist_name', 'title'])
        print(f'Tracks identified to scrape lyrics: {self.df.shape[0]}')

    def populate_nillboard_scrapables(self):
        pass


    def scrape_df_segment_to_db(self, scraperange, verbose=1):
        df = self.df.copy()
        for i in scraperange:
            row = df.iloc[i]
            try:
                self.scrape_song_to_db(row['artist_name'], row['title'], row['track_id'])
            except TypeError as e:
                self.record_error(row['track_id'], 'TypeError')
            except DuplicateKeyError: 
                if verbose:
                    print(f'Duplicate skipped on index {i}')
            if verbose > 1:
                print(i)

    def scrape_song_to_db(self, artist, title, track_id):
        artist=stripFeat(artist)
        try:
            #record stout from lyricsgenius call because it catches errors and prints
            with Capturing() as output:
                songdata = self.api.search_song(title, artist)

        #for the few errors that have been raised
        except ReadTimeout:
            self.api.sleep_time += 3
            print(f'sleep time increased to {self.api.sleep_time}')
            self.record_error(track_id, 'ReadTimeout')
            self.scrape_song_to_db(artist, title, track_id)
            return

        #take sleep time slowly back to minimum
        if self.api.sleep_time > self.minsleep:
            self.api.sleep_time -= 0.25
            print(f'sleep time decreased to {self.api.sleep_time}')
        
        #search successful
        if songdata != None:
            self.record_lyrics_result(track_id, songdata)

        #handle (record & retry) Timeout error
        elif output[1].startswith('Timeout'):
            self.api.sleep_time += 3 
            self.record_error(track_id, 'Timeout')
            self.scrape_song_to_db(artist, title, track_id)
            return

        #record error: not in genius db
        elif output[1].startswith('No results'):
            self.record_error(track_id, 'no_results')
        
        #record error: song without lyrics
        elif output[1] == 'Specified song does not contain lyrics. Rejecting.':
            self.record_error(track_id, 'lacks_lyrics')
        
        #record error: URL issue
        elif output[1] == 'Specified song does not have a valid URL with lyrics. Rejecting.': 
            self.record_error(track_id, 'invalid_url')

    def record_lyrics_result(self, track_id, songdata):
        self.lyrics.insert_one({
            '_id': track_id,
            'response_artist': songdata.artist,
            'response_title': songdata.title,
            'lyrics': songdata.lyrics})

    def record_error(self, track_id, errtype):
        self.errlog.insert_one({
            'track': track_id,
            'type': errtype})

    def record_error_verbose(self, track_id, errmsg):
        self.errlog.insert_one({
            'track': track_id,
            'type': 'verbose',
            'message': errmsg})

def stripFeat(s):
    if ' Featuring' in s:
        return s[:s.index(' Featuring')]
    elif ' x ' in s:
        return s[:s.index(' x ')]
    else:       
        return s 

if __name__ == '__main__':
    s = Scraper()
    s.populate_billboard_scrapables()
    scraperange = range(0,s.df.shape[0])
    s.scrape_df_segment_to_db(scraperange,2)

    '''
    end = s.df.shape[0]
    mode = int(sys.argv[1])
    if mode == 0:
        for start in range(100000,100005):
            scraperange = range(start, end, 10) 
            s.scrape_df_segment_to_db(scraperange, verbose=True)
    elif mode == 5:
        for start in range(100005, 100010):
            scraperange = range(start, end, 10)
            s.scrape_df_segment_to_db(scraperange, verbose=True)
    '''
    '''
    df_read = pd.read_csv('data/Billboard_MSD_Matches.csv', index_col=0)
    df_read = df_read[df_read['msdid']!='']
    df_to_read = pd.read_csv('data/All_Billboard_MSD_Matches.csv', index_col=0)

    read_msdids = set(df_read['msdid'].values)
    mask = df_to_read['msdid'].apply(lambda x: x not in read_msdids)
    df_to_read = df_to_read[mask]

    s.df = df_to_read.rename(columns={'artist':'artist_name', 'track':'title', 'msdid':'track_id'})
    scraperange = range(s.df.shape[0])
    s.scrape_df_segment_to_db(scraperange, verbose=True)
    '''
