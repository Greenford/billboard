import pandas as pd
import numpy as np
from pymongo import MongoClient
from functools import reduce
from operator import add


class BillboardData(object):
    def __init__(self):
        db = MongoClient().billboard
        df1 = load_spotify_billboard_data(db)
        df2 = load_spotify_nillboard_data(db)
        lyrics_df = load_lyrics_data(db)
        adf = load_spotify_album_data(db)
        bbdf = load_hot_100_data(db)
        self.df = (
            df1.append(df2, ignore_index=True, sort=False)
            .merge(right=lyrics_df, how="outer", on="track_id")
            .merge(right=adf, how="left", on="album_id")
            .merge(right=bbdf, how="left", on="obj_id")
        )

    def load_spotify_bb_data(db):
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
                    r["_id"],
                ],
                db.spotify.find(),
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

    def load_spotify_nillboard_data(db):
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
                db.spotify_nillboard.find(),
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

    def load_lyrics_data(db):
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
                db.lyrics.find(),
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

    def load_hot_100_data(db):
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
                db.hot100filtered.find(),
            ),
            columns=["obj_id", "bb_artist", "bb_title", "date", "peakPos", "weeks"],
        )

    def load_spotify_album_data(db):
        # None of the genres are filled
        adf = pd.DataFrame(
            map(
                lambda r: [r["_id"], r["label"], r["popularity"]],
                db.spotify_albums.find(),
            ),
            columns=["album_id", "label", "album_popularity",],
        )

        self.df = (
            df1.append(df2, ignore_index=True, sort=False)
            .merge(right=lyrics_df, how="outer", on="track_id")
            .merge(right=adf, how="left", on="album_id")
            .merge(right=bbdf, how="left", on="obj_id")
        )

    def transform_for_models(self):
        self.df["on_billboard"] = ~self.df.obj_id.isna()
        self.df.release_date = pd.to_datetime(self.df.release_date, format="%Y-%m-%d")
        self.df["norm_sentiment"] = (self.df.poscount - self.df.negcount) / (
            self.df.poscount + self.df.negcount + 1
        )
        self.df["rel_sentiment"] = (self.df.poscount - self.df.negcount) / (
            self.df.wordcount
        )
        self.df["release_year"] = self.df.release_date.apply(lambda dt: dt.year)
        self.df["release_month"] = self.df.apply(
            lambda r: r.release_date.month
            if r.release_date_precision == "day"
            else np.nan,
            axis=1,
        )

    def transform_for_EDA(self):
        pass
