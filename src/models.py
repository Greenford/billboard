from src.pipe import BillboardData

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from functools import reduce
from operator import add
from collections import Counter
import pickle

import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_validate, train_test_split
from sklearn.inspection import permutation_importance

plt.style.use("fivethirtyeight")


def present_cv(cv):
    """
    Shows the results of a cross_validate. 

    Args: 
        cv (dict): results from a cross_validate call)

    Returns: pandas dataframe with the results. Meant to be displayed in a
    Jupyter Notebook. 
    """
    arr = []
    for key in cv.keys():
        mu = np.mean(cv[key])
        st = np.std(cv[key])
        arr.append([key, mu, st])
    return pd.DataFrame(arr, columns=["metric", "mean", "std"])


def plot_feature_importances(model, feature_labels, max_feats=10):
    """
    Plots feature importances from a fit Random Forest or Gradient Booster. 

    Args:
        model (ML model): model to get the feature importances from.
        feature_labels (list of str): names of the features
        max_feats (int): max number of features to plot. Default: 10
    """
    n_columns = len(feature_labels)
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]
    if isinstance(model, RandomForestClassifier):
        std = np.std([tree.feature_importances_ for tree in model.estimators_], axis=0)
        yerr = std[indices]
    else:
        yerr = None

    # Plot the feature importances of the forest
    plt.figure(figsize=(15, 15))
    plt.title("Feature importances")
    plt.bar(
        range(n_columns), importances[indices], color="r", align="center", yerr=yerr
    )
    plt.xticks(
        range(min(n_columns, max_feats)), feature_labels[indices], rotation="vertical"
    )
    plt.xlim([-1, min(n_columns, max_feats)])

    plt.show()


def plot_perm_importances(model, X, y, feature_labels, max_feats=10, logscale=False):
    """
    Computes and plots the permutation importances for a model and data.

    Args:
        model (ML model): model to get permutation importances of. Needs fit
        and score methods. 
        X (numpy ndarray): data columns to permute.
        y (numpy ndarray): target 
        feature_labels (list of str): labels for the columns in X. 
        max_feats (int): max number of features to plot. Default 10
        logscale (bool): True if plotting on log y-axis. Default False. 
    """
    pi = permutation_importance(
        model, X, y, scoring="accuracy", n_jobs=-1, n_repeats=10
    )
    indices = np.argsort(pi.importances_mean)[::-1]
    std = pi.importances_std
    yerr = std[indices]

    # Plot the feature importances of the forest
    plt.figure(figsize=(10, 6))
    plt.title("Permutation importances")
    plt.boxplot(pi.importances[indices].T)
    plt.xticks(
        range(1, min(X.shape[1], max_feats) + 1),
        feature_labels[indices],
        rotation="vertical",
    )
    plt.xlim([-1, min(X.shape[1], max_feats) + 2])
    if logscale:
        plt.yscale("log")
    plt.ylabel("Importance")
    plt.xlabel("Feature")
    plt.show()
