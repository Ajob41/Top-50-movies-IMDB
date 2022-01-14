import logging
import re
from datetime import datetime

import numpy as np
import pandas as pd

import util

log = logging.getLogger(__name__)


def get_quarter_code(row):
    if row['month_code'] <= 3:
        return 1
    elif row['month_code'] <= 6:
        return 2
    elif row['month_code'] <= 9:
        return 3
    else:
        return 4


def clean_release_year(row):
    return int(re.sub("[^0-9]", "", row["release_year"]))


def transform_staging_table(raw_movie_df):
    # instantiate movie performance staging class
    movie_perf_raw = MoviePerformanceStaging(raw_movie_df)
    # transform time dimensions
    movie_perf_time = movie_perf_raw.transform_time_dimensions()
    # transform movie dimensions
    movie_perf_time_movie = movie_perf_time.transform_movie_dimensions()
    # transform fact table elements
    movie_perf_time_movie_fact = movie_perf_time_movie.transform_fact_elements()
    # remove elements that are not needed
    movie_pref_staging = movie_perf_time_movie_fact.remove_unneeded_columns()

    log.info("Staging transformations completed")

    return movie_pref_staging


def ingest_new_staging_data(path, conn_str):
    # read gzip csv from folder
    raw_data_df = util.read_all_csv_to_df(path)
    # transform raw data
    staging_table_obj = transform_staging_table(raw_data_df)
    # insert transformed data into staging (replaces old data)
    util.ingest_df_into_sql(staging_table_obj.staging_df, conn_str, "movie_performance_staging", "replace")
    log.info("Staging data is loaded")


class MoviePerformanceStaging(object):
    def __init__(self, df: pd.DataFrame):
        # combine movies that were in different top 50 genre lists
        self.staging_df = df.groupby(['title', 'url', 'release_year', 'mpaa_rating', 'runtime_minutes',
                                      'genres', 'imdb_rating', 'metascore_rating', 'actors', 'directors',
                                      'summary', 'num_votes', 'gross_earnings', 'file_name',
                                      'timestamp'], dropna=False).agg({'imdb_rank': ','.join}).reset_index()

    def transform_time_dimensions(self):
        self.staging_df["day_key"] = np.nan
        self.staging_df["capture_date"] \
            = self.staging_df.apply(lambda row: datetime.strptime(row.timestamp, "%Y-%m-%d %H:%M:%S.%f").date(), axis=1)
        self.staging_df["month_code"] \
            = self.staging_df.apply(lambda row: datetime.strptime(row.timestamp, "%Y-%m-%d %H:%M:%S.%f").month, axis=1)
        self.staging_df["year"] \
            = self.staging_df.apply(lambda row: datetime.strptime(row.timestamp, "%Y-%m-%d %H:%M:%S.%f").year, axis=1)
        self.staging_df["quarter_code"] \
            = self.staging_df.apply(lambda row: get_quarter_code(row), axis=1)

        return self

    def transform_movie_dimensions(self):
        self.staging_df["movie_key"] = np.nan

        self.staging_df["imdb_id"] = self.staging_df.apply(lambda row: row.url.split("/")[2], axis=1)

        self.staging_df["runtime_minutes"] = pd.to_numeric(self.staging_df["runtime_minutes"].str.replace(" min", ""),
                                                           errors='coerce')

        self.staging_df["release_year"] = self.staging_df.apply(lambda row: clean_release_year(row), axis=1)

        return self

    def transform_fact_elements(self):
        self.staging_df["gross_earnings"] = pd.to_numeric(self.staging_df["gross_earnings"].str.replace(",", ""),
                                                          errors='coerce')
        self.staging_df["metascore_rating"] = pd.to_numeric(self.staging_df["metascore_rating"], errors='coerce')
        self.staging_df["imdb_rating"] = pd.to_numeric(self.staging_df["imdb_rating"], errors='coerce')
        self.staging_df["num_votes"] = pd.to_numeric(self.staging_df["num_votes"], errors='coerce')
        self.staging_df["timestamp"] = pd.to_datetime(self.staging_df["timestamp"], errors='coerce')

        return self

    def remove_unneeded_columns(self):
        cols_to_remove = ["url"]
        for col in cols_to_remove:
            self.staging_df.drop(col, inplace=True, axis=1)

        return self
