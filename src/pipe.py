import pandas as pd
import numpy as np
from pymongo import MongoClient
from functools import reduce
from operator import add
from collections import defaultdict, Counter

from sklearn.model_selection import train_test_split


class BillboardData:
    """
    Loads, aggregates, and transforms data related to the Billboard 100 project.
    Typical usage: 
    bbd = BillboardData()
    bbd.load()
    bbd.transform...()
    """

    def __init__(self):
        self.db = MongoClient().billboard
        self.df = None

    def load(self):
        """
        Loads Spotify, Genius, and Billboard data from a local MongoDB into self.df.
        """
        # loads all
        df1 = self.load_spotify_billboard_data()
        df2 = self.load_spotify_nillboard_data()
        adf = self.load_spotify_album_data()
        bbdf = self.load_hot_100_data()
        lyrics_df = self.load_lyrics_data()

        # combines all the dataframes
        self.df = (
            df1.append(df2, ignore_index=True, sort=False)
            .merge(right=lyrics_df, how="left", on="track_id")
            .merge(right=adf, how="left", on="album_id")
            .merge(right=bbdf, how="left", on="obj_id")
        )

    def load_spotify_billboard_data(self):
        """
        Loads all the Spotify Billboard data. 

        Returns: Dataframe of the Spotify Billboard Data. 
        """
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
                self.db.spotify.find(),
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

    def load_spotify_nillboard_data(self):
        """
        Loads all the Spotify Nillboard data.

        Returns: Dataframe of the Spotify Nillboard.
        """

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
                    None,  # no obj_id because these tracks were not on the billboard.
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
        """
        Loads all the lyrics data.
        Returns: Dataframe of the lyrics data (lyrics not included).
        """
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
                self.db.lyrics.find({"dict_sentiment.wordcount": {"$exists": "true"}}),
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
        """
        Loads the Billboard Hot 100 data.
        Returns: Dataframe of the Hot 100 data.
        """
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
            columns=[
                "obj_id",
                "bb_artist",
                "bb_title",
                "date_entered_bb",
                "peakPos",
                "weeks",
            ],
        )

    def load_spotify_album_data(self):
        """
        Loads spotify album data (genre data not included - only empty
        lists returned from Spotify API.

        Returns: Dataframe of the album data. 
        """
        # None of the genres are filled
        return pd.DataFrame(
            map(
                lambda r: [r["_id"], r["label"], r["popularity"]],
                self.db.spotify_albums.find(),
            ),
            columns=["album_id", "label", "album_popularity",],
        )

    def drop_no_lyrics(self):
        """
        Drops rows in the dataframe without lyrics. 
        """
        haslyrics = ~self.df.response_title.isna()
        self.df = self.df[haslyrics].reset_index(drop=True)

    def split_test(self, test_size=0.1, rstate=None):
        """
        Splits off a portion of the data for testing.

        Args: 
            test_size (float) fraction of the data to return.
            rstate (int): random_state for the split

        Returns: (Pandas DataFrame) test data. self.df becomes what remains. 
        """

        self.df, test = train_test_split(self.df, test_size=test_size, random_state=rstate)
        return test 

    def transform_for_models(self):
        """
        Transforms self.df for machine learning models. 
        """

        # Makes the target column
        self.df["on_billboard"] = ~self.df.obj_id.isna()

        # converts the date-string to a datetime
        self.df.release_date = pd.to_datetime(self.df.release_date, format="%Y-%m-%d")

        # breaks out the year and month from the date
        if "release_year" not in self.df.columns:
            self.df["release_year"] = self.df.release_date.apply(lambda dt: dt.year)
            self.df["release_month"] = self.df.apply(
                lambda r: r.release_date.month
                if r.release_date_precision == "day"
                else np.nan,
                axis=1,
            )

        # computes the lyrical sentiment from the related fields.
        self.df["lyric_sentiment"] = (self.df.poscount - self.df.negcount) / (
            self.df.poscount + self.df.negcount + 1
        )

        # Computes the track placement
        self.df["track_placement"] = self.df.apply(
            lambda r: (r.track_number / r.total_tracks + 1 - 1 / r.disc_number)
            if r.total_tracks > 1
            else -1,  # no sense in adding singles to track placement
            axis=1,
        )

        # Drop unneeded columns
        self.df.drop(
            columns=[
                "track_id",
                "obj_id",
                "poscount",
                "negcount",
                "artist",
                "album_id",
                "release_date_precision",
                "release_date",
                "disc_number",
                "track_number",
                "title",
                "response_artist",
                "response_title",
                "bb_artist",
                "bb_title",
                "peakPos",
                "weeks",
                "date_entered_bb",
                "album_type",
            ],
            inplace=True,
        )

    def balance_class_year(self, rseed=None):
        """
        Balances target class of self.df by year using random sampling. 

        Args: 
            rseed (int): numpy random seed used for reproducibility. Seed is 
            reset after sampling.
        """

        if "release_year" not in self.df.columns:
            self.df.release_date = pd.to_datetime(
                self.df.release_date, format="%Y-%m-%d"
            )
            self.df["release_year"] = self.df.release_date.apply(lambda dt: dt.year)
            self.df["release_month"] = self.df.apply(
                lambda r: r.release_date.month
                if r.release_date_precision == "day"
                else -1,
                axis=1,
            )
        # separate classes temporarily
        bbmask = ~self.df.obj_id.isna()
        bbdf = self.df[bbmask]
        nbdf = self.df[~bbmask]

        # get target counts for each year
        yearcounts = bbdf.groupby("release_year").count()["energy"].to_dict()

        # actually do the random sampling
        chosen_ids = []
        np.random.seed(rseed)
        for year in range(2000, 2020):
            avail_ids = nbdf[nbdf.release_year == year]["track_id"].values
            chosen_ids.extend(
                np.random.choice(avail_ids, yearcounts[year], replace=False)
            )

        chosen_ids = set(chosen_ids)
        nbdf = nbdf[nbdf.track_id.apply(lambda i: i in chosen_ids)]

        self.df = bbdf.append(nbdf, ignore_index=True, sort=False)

        # reset random seed
        np.random.seed(None)

    def transform_label_to_hitcount(self, testdf=None):
        """
        Transforms the record label names to the number of hits they have 
        on the billboard. This is a post test-train-split transform.

        Args:
            testdf (Pandas DataFrame): test split data if it needs to be transformed. 
            Default None, which transforms the internal (training) data.

        Returns: None if testdf is None, the transformed DataFrame otherwise
        """

        # train mode. 
        if type(testdf)==type(None):
            self.df.label = self.df.label.apply(
                lambda l: ["".join(lword.split()).lower() for lword in l.split("/")]
            )
            hitdf = self.df[self.df.on_billboard == 1]

            self.label_hitcount = Counter(reduce(add, hitdf.label.values))

            self.df.label = self.df.label.apply(
                lambda l: np.mean([self.label_hitcount[lab] for lab in l])
            ).astype(int)
        
        # test mode
        else: 
            testdf.label = testdf.label.apply(
                lambda l: ["".join(lword.split()).lower() for lword in l.split("/")]
            )

            testdf.label = testdf.label.apply(
                lambda l: np.mean([self.label_hitcount[lab] for lab in l])
            ).astype(int)
            return testdf

    def scale(self):
        """
        Off-the-cuff scaling function to use with a logistic regressor. 
        """
        scale_f = {
            "duration_ms": lambda ms: ms * 1.0 / 60000,
            "popularity": lambda p: p / 100,
            "album_popularity": lambda p: p / 100,
            "label": lambda l: l / 100,
            "wordcount": lambda wc: wc / 100,
            "release_year": lambda y: y - 2000,
            "tempo": lambda t: t / 100,
        }
        for col in self.df.columns:
            if col in scale_f:
                self.df[col] = self.df[col].apply(scale_f[col])

    def drop_popularities(self):
        """
        Drops the two measures of popularity. 
        """
        self.df.drop(columns=["popularity", "album_popularity"], inplace=True)
 

# style kwargs for the next two functions
plotstyle = {
    "color": "black",
    "ecolor": "red",
    "elinewidth": 1,
}


def plotbyyear(df, col, axx=None):
    """
    Plots a column from a dataframe by year with error bars.

    Args:
        df (Pandas Dataframe): Dataframe to plot from
        col (str): column from the dataframe to plot.
        axx (pyplot.axes): axes object to plot on.
    """
    gb = df[["release_year", col]].groupby("release_year")
    mu = gb.mean()
    s = gb.std()
    if not axx:
        _, axx = plt.subplots()
    axx.errorbar(mu.index, mu.values, yerr=s.values, **plotstyle)
    axx.set_title(f"{col} vs Release Year")
    axx.set_xlabel("Year")
    axx.set_ylabel(col)


def plotbymonth(df, col, axx=None):
    """
    Plots a column from a dataframe by month with error bars.

    Args:
        df (Pandas Dataframe): Dataframe to plot from
        col (str): column from the dataframe to plot.
        axx (pyplot.axes): axes object to plot on.
    """
    gb = df[["month", col]].groupby("month")
    mu = gb.mean()
    s = gb.std()
    if not axx:
        _, axx = plt.subplots()
    axx.set_title(f"{col} vs Release Month")
    axx.set_xlabel("Year")
    axx.set_ylabel(col)
    axx.errorbar(mu.index, mu.values, yerr=s.values, **plotstyle)


def scatter_2feat(df, xax, yax):
    """
    Makes a scatterplot of two columns within a dataframe.

    Args: 
        df (Pandas Dataframe): dataframe to plot from
        xax (str): column from df to be the x-axis
        yax (str): column from df to be the y-axis

    Returns: pyplot.axes object with scatterplot. 
    """
    bbstyle = {"label": "Billboard", "color": "red", "alpha": 0.1}
    nbstyle = {
        "label": "Not Billboard",
        "color": "black",
        "alpha": 0.1,
    }

    fig, ax = plt.subplots(figsize=(10, 10))

    bbmask = df.on_billboard
    ax.scatter(df[~bbmask][xax], df[~bbmask][yax], **nbstyle)
    ax.scatter(df[bbmask][xax], df[bbmask][yax], **bbstyle)

    ax.set_xlabel(xax)
    ax.set_ylabel(yax)
    ax.set_title(xax + " vs " + yax)
    ax.legend()
    return ax
