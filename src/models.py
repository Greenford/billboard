from src.pipe import BillboardData
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')

import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_validate, train_test_split
from sklearn.inspection import permutation_importance

from functools import reduce
from operator import add
from collections import Counter

import pickle

"""
class MyModel:
    def __init__(self, estimator, ekwargs):
        self.est = estimator(**ekwargs)
    def fit(self, X, y):
        X.insert(X.shape[1], 'on_billboard', y)
        df = X.copy()
        df.release_date = pd.to_datetime(df.release_date, format="%Y-%m-%d")
        df["norm_sentiment"] = (df.poscount - df.negcount) / (
            df.poscount + df.negcount + 1
        )
        df["release_year"] = df.release_date.apply(lambda dt: dt.year)
        df["release_month"] = df.apply(
            lambda r: r.release_date.month
            if r.release_date_precision == "day"
            else np.nan,
            axis=1
        )
        df['track_placement'] = df.track_number/df.total_tracks + 1 - 1/df.disc_number
        df.explicit = df.explicit.astype(np.uint8)
        df.on_billboard = df.on_billboard.astype(np.uint8)
        df = pd.get_dummies(df, columns=[
            'album_type', 'key', 'time_signature', 'release_month'])
        
        
        df.label = df.label.apply(
            lambda l: [''.join(lword.split()).lower() for lword in l.split('/')]
        )
        hitdf = df[df.on_billboard==1]
        self.label_hitcount = Counter(reduce(add,hitdf.label.values))
        df.label = df.label.apply(
            lambda l: np.mean([self.label_hitcount[lab] for lab in l])
        ).astype(int)

        # Drop unneeded columns
        df.drop(columns=[
            'poscount',
            'negcount',
            'release_date_precision',
            'release_date',
            'disc_number',
            'track_number',
            'album_type_album',
            'key_0',
            'time_signature_0',
            'release_month_1.0',
        ], inplace=True)

        y = df.pop('on_billboard').values
        df = df.reindex(sorted(df.columns), axis=1)
        X = df.values 
        self.fitX = df.copy()
        self.est.fit(X, y)

    def _fix_predict_X(self, X):
        X.release_date = pd.to_datetime(X.release_date, format="%Y-%m-%d")
        X["release_year"] = X.release_date.apply(lambda dt: dt.year)
        X["release_month"] = X.apply(
            lambda r: r.release_date.month
            if r.release_date_precision == "day"
            else np.nan,
            axis=1
        )
        X['track_placement'] = X.track_number/X.total_tracks + 1 - 1/X.disc_number
        X["norm_sentiment"] = (X.poscount - X.negcount) / (
            X.poscount + X.negcount + 1
        )
        X.explicit = X.explicit.astype(np.uint8)
        
        for col in ['album_type_compilation', 'album_type_single', 
                
               'key_1', 'key_2', 'key_3', 'key_4',
               'key_5', 'key_6', 'key_7', 'key_8', 'key_9', 'key_10', 'key_11',
               
               'time_signature_1', 'time_signature_3',
               'time_signature_4', 'time_signature_5', 
               
               'release_month_2.0', 'release_month_3.0', 'release_month_4.0',
               'release_month_5.0', 'release_month_6.0', 'release_month_7.0',
               'release_month_8.0', 'release_month_9.0', 'release_month_10.0',
               'release_month_11.0', 'release_month_12.0']:
            X.insert(X.shape[1], col, 0)
        for alb_type in ['compilation', 'single']:
            X[f'album_type_{alb_type}'] = (X.album_type==alb_type).astype(np.uint8)
        for key in range(1, 12):
            X[f'key_{key}'] = (X.key == key).astype(np.uint8)
        for ts in [1, 3, 4, 5]:
            X[f'time_signature_{ts}'] = (X.time_signature==ts).astype(np.uint8)
        for month in range(2,13):
            X[f'release_month_{float(month)}'] = (X.release_month==float(month)).astype(np.uint8)

        X.drop(columns=[
            'album_type',
            'key',
            'time_signature',
            'release_month',
            'poscount',
            'negcount',
            'release_date_precision',
            'release_date',
            'disc_number',
            'track_number',
        ], inplace=True)

        X.label = X.label.apply(
            lambda l: [''.join(lword.split()).lower() for lword in l.split('/')]
        )
        X.label = X.label.apply(
            lambda l: np.mean([self.label_hitcount[lab] for lab in l])
        ).astype(int)
        X = X.reindex(sorted(X.columns), axis=1)
        self.predX = X
        return X

    def predict_proba(self, X):
        X = self._fix_predict_X(X)
        return self.est.predict_proba(X.values)

    def predict(self, X):
        X = self._fix_predict_X(X)
        return self.est.predict(X.values)


def get_data():
    df = BillboardData().df 
    df["on_billboard"] = ~df.obj_id.isna()
    haslyrics = ~df.response_title.isna()
    df = df[haslyrics].reset_index(drop=True) 
    df.drop(columns=[
            'track_id',
            'obj_id',
            'artist',
            'album_id',
            'title',
            'response_artist',
            'response_title',
            'bb_artist',
            'bb_title',
            'peakPos',
            'weeks',
            'date_entered_bb',
        ], inplace=True)


    y = df.pop('on_billboard')
    X = df
    return X, y
"""

def present_cv(cv):
    arr=[]
    for key in cv.keys():
        mu = np.mean(cv[key])
        st = np.std(cv[key])
        arr.append([key, mu, st])
    return pd.DataFrame(arr, columns=['metric', 'mean', 'std'])

def plot_feature_importances(X, model, max_feats, feature_labels):
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]
    if isinstance(model, RandomForestClassifier):
        std = np.std([tree.feature_importances_ for tree in model.estimators_], axis=0)
        yerr = std[indices]
    else:
        yerr=None

    # Plot the feature importances of the forest
    plt.figure(figsize=(15,15))
    plt.title("Feature importances")
    plt.bar(range(X.shape[1]), importances[indices], color="r", align="center", yerr=yerr)
    plt.xticks(range(min(X.shape[1], max_feats)), feature_labels[indices], rotation='vertical')
    plt.xlim([-1, min(X.shape[1], max_feats)])

    plt.show()

def plot_perm_importances(model, X, y, max_feats, feature_labels):
    pi = permutation_importance(model, X, y, scoring='accuracy', n_jobs=-1, n_repeats=10)
    indices = np.argsort(pi.importances_mean)[::-1]
    std = pi.importances_std
    yerr = std[indices]

    # Plot the feature importances of the forest
    plt.figure(figsize=(15,15))
    plt.title("Permutation importances")
    plt.boxplot(pi.importances[indices])
    plt.xticks(range(min(X.shape[1], max_feats)), feature_labels[indices], rotation='vertical')

    plt.show()

"""
if __name__ == '__main__':
    X, y = get_data()
    model = MyModel()
    model.fit(X, y)
    with open('model.pkl', 'wb') as f:
        # Write the model to a file.
        pickle.dump(model, f)

"""
