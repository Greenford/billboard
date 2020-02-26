import pandas as pd
import numpy as np
from pymongo import MongoClient
from functools import reduce
from operator import add
from sklearn.feature_extraction.text import CountVectorizer

class BillboardData(object):
    def __init__(self):
        self.db = MongoClient().billboard
        df1 = self.load_spotify_billboard_data()
        df2 = self.load_spotify_nillboard_data()
        lyrics_df = self.load_lyrics_data()
        adf = self.load_spotify_album_data()
        bbdf = self.load_hot_100_data()
        self.df = df1.append(df2, ignore_index=True, sort=False)\
            .merge(right=lyrics_df, how="outer", on="track_id")\
            .merge(right=adf, how="left", on="album_id")\
            .merge(right=bbdf, how="left", on="obj_id")

    def load_spotify_billboard_data(self):
        return pd.DataFrame(
            map(
                lambda r: [
                    r["metadata"]["artists"][0]["name"],
                    r["metadata"]["album"]["id"],
                    r["metadata"]["album"]["album_type"],
                    r["metadata"]["album"]["total_tracks"],
                    r["metadata"]["album"]["release_date"],
                    r["metadata"]["album"]["release_date_precision"],
                    r["metadata"]["disc_number"],
                    r["metadata"]["duration_ms"],
                    r["metadata"]["explicit"],
                    r["metadata"]["id"],
                    r["metadata"]["name"],
                    r["metadata"]["popularity"],
                    r["metadata"]["track_number"],
                    r["audio_features"]["danceability"],
                    r["audio_features"]["energy"],
                    r["audio_features"]["acousticness"],
                    r["audio_features"]["key"],
                    r["audio_features"]["loudness"],
                    r["audio_features"]["mode"],
                    r["audio_features"]["speechiness"],
                    r["audio_features"]["instrumentalness"],
                    r["audio_features"]["liveness"],
                    r["audio_features"]["valence"],
                    r["audio_features"]["tempo"],
                    r["audio_features"]["time_signature"],
                    r["_id"]
                ],
                self.db.spotify.find()
            ),
            columns=[
                "artist",
                "album_id",
                "album_type",
                "total_tracks",
                "release_date",
                "release_date_precision",
                "disc_number",
                "duration_ms",
                "explicit",
                "track_id",
                "title",
                "popularity",
                "track_number",
                "danceability",
                "energy",
                "acousticness",
                "key",
                "loudness",
                "mode",
                "speechiness",
                "instrumentalness",
                "liveness",
                "valence",
                "tempo",
                "time_signature",
                "obj_id"
            ]
        )

    def load_spotify_nillboard_data(self):
        return pd.DataFrame(
            map(
                lambda r: [
                    r["metadata"]["artists"][0]["name"],
                    r["metadata"]["album"]["id"],
                    r["metadata"]["album"]["album_type"],
                    r["metadata"]["album"]["total_tracks"],
                    r["metadata"]["album"]["release_date"],
                    r["metadata"]["album"]["release_date_precision"],
                    r["metadata"]["disc_number"],
                    r["metadata"]["duration_ms"],
                    r["metadata"]["explicit"],
                    r["metadata"]["id"],
                    r["metadata"]["name"],
                    r["metadata"]["popularity"],
                    r["metadata"]["track_number"],
                    r["audio_features"]["danceability"],
                    r["audio_features"]["energy"],
                    r["audio_features"]["acousticness"],
                    r["audio_features"]["key"],
                    r["audio_features"]["loudness"],
                    r["audio_features"]["mode"],
                    r["audio_features"]["speechiness"],
                    r["audio_features"]["instrumentalness"],
                    r["audio_features"]["liveness"],
                    r["audio_features"]["valence"],
                    r["audio_features"]["tempo"],
                    r["audio_features"]["time_signature"],
                    None,
                ],
                self.db.spotify_nillboard.find(),
            ),
            columns=[
                "artist",
                "album_id",
                "album_type",
                "total_tracks",
                "release_date",
                "release_date_precision",
                "disc_number",
                "duration_ms",
                "explicit",
                "track_id",
                "title",
                "popularity",
                "track_number",
                "danceability",
                "energy",
                "acousticness",
                "key",
                "loudness",
                "mode",
                "speechiness",
                "instrumentalness",
                "liveness",
                "valence",
                "tempo",
                "time_signature",
                "obj_id",
            ],
        )

    def load_lyrics_data(self):
        return pd.DataFrame(
            map(
                lambda r: [
                    r["_id"],
                    r["response_artist"],
                    r["response_title"],
                    r["dict_sentiment"]["pos"],
                    r["dict_sentiment"]["neg"],
                    r["dict_sentiment"]["wordcount"],
                ],
                self.db.lyrics.find(),
            ),
            columns=[
                "track_id",
                "response_artist",
                "response_title",
                "poscount",
                "negcount",
                "wordcount",
            ],
        )

    def load_hot_100_data(self):
        return pd.DataFrame(
            map(
                lambda r: [
                    r["_id"],
                    r["artist"],
                    r["title"],
                    r["date"],
                    r["peakPos"],
                    r["weeks"],
                ],
                self.db.hot100filtered.find(),
            ),
            columns=["obj_id", "bb_artist", "bb_title", "date_entered_bb", "peakPos", "weeks"]
        )

    def load_spotify_album_data(self):
        # None of the genres are filled
        return pd.DataFrame(
            map(
                lambda r: [r["_id"], r["label"], r["popularity"]],
                self.db.spotify_albums.find(),
            ),
            columns=["album_id", "label", "album_popularity",],
        )

    def transform_for_models(self):
        # Drop if they don't have lyrics 
        haslyrics = ~self.df.response_title.isna()
        self.df = self.df[haslyrics].reset_index(drop=True)
        

        self.df["on_billboard"] = ~self.df.obj_id.isna()
        self.df.release_date = pd.to_datetime(self.df.release_date, format="%Y-%m-%d")
        self.df["norm_sentiment"] = (self.df.poscount - self.df.negcount) / (
            self.df.poscount + self.df.negcount + 1
        )
        self.df["release_year"] = self.df.release_date.apply(lambda dt: dt.year)
        self.df["release_month"] = self.df.apply(
            lambda r: r.release_date.month
            if r.release_date_precision == "day"
            else np.nan,
            axis=1
        )
        self.df['track_placement'] = self.df.track_number/self.df.total_tracks + 1 - 1/self.df.disc_number
        self.df.explicit = self.df.explicit.astype(np.uint8)
        self.df.on_billboard = self.df.on_billboard.astype(np.uint8)
        self.df = pd.get_dummies(self.df, columns=['album_type', 'key', 'time_signature', 'release_month'])
        # Drop unneeded columns
        self.df.drop(columns=[
            'track_id',
            'obj_id',
            'poscount',
            'negcount',
            'artist',
            'album_id',
            'release_date_precision',
            'release_date',
            'disc_number',
            'track_number',
            'title',
            'response_artist',
            'response_title',
            'bb_artist',
            'bb_title',
            'peakPos',
            'weeks',
            'date_entered_bb',
        ], inplace=True)

    def dummyize_record_label(self, min_label_size=12):
        # Turns each track's list of labels into a space-separated string of labels
        self.df.label = self.df.label.apply(lambda l: " ".join(
            [''.join(lword.split()) for lword in l.split('/')]
        ).lower())
        vectorizor = CountVectorizer()
        counts = vectorizor.fit_transform(self.df.label).toarray()

        counts = pd.DataFrame(counts, columns = map(lambda s: 'label_'+s, 
            vectorizor.get_feature_names()), dtype=np.uint8)
        labels_start_idx = self.df.shape[1]
        self.df = self.df.merge(counts, left_index=True, right_index=True)

        lt_label=f'lt_{min_label_size}_label'
        self.df.insert(labels_start_idx, lt_label, 0)
        labels_start_idx += 1
        
        for column in list(self.df.columns[labels_start_idx:]):
            if np.sum(self.df[column]) < 12:
                self.df[lt_label] = self.df[lt_label]+self.df[column]
                self.df.drop(columns=column, inplace=True, axis=1)
        self.df.drop(columns='label', inplace=True, axis=1)

    def drop_popularities(self):
        self.df.drop(columns=['popularity', 'album_popularity'], inplace=True)

    def transform_for_EDA(self):
        pass
