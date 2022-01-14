import glob
import logging
import os
import shutil

import pandas as pd
from sqlalchemy import create_engine, exc

log = logging.getLogger(__name__)


def read_all_csv_to_df(path):
    all_files = glob.glob(path + "/*.csv.gz")
    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        df['file_name'] = os.path.basename(filename)
        li.append(df)

    return pd.concat(li, axis=0, ignore_index=True)


def ingest_df_into_sql(df, conn_str, table_name, action_if_exists):
    try:
        engine = create_engine(conn_str)
        df.to_sql(table_name, engine, if_exists=action_if_exists, index=False)
    except (exc.SQLAlchemyError, BaseException) as e:
        log.error("df: %s connection string: %s table name: %s action: %s Error: %s"
                  % (df, conn_str, table_name, action_if_exists, e))
        raise


def run_crud_operation(conn_str, stmt):
    try:
        engine = create_engine(conn_str)
        conn = engine.connect()
        result = conn.execute(stmt)
        conn.close()
        return result
    except exc.SQLAlchemyError as e:
        log.error("connection string: %s statement: %s Error: %s" % (conn_str, stmt, e))
        raise


def read_sql_into_df(conn_str, query):
    try:
        engine = create_engine(conn_str)
        return pd.read_sql_query(query, con=engine)
    except BaseException as e:
        log.error("connection string: %s query: %s Error: %s" % (conn_str, query, e))
        raise


def archive_old_files(source_dir, target_dir):
    try:
        # list all files in source directory
        file_names = os.listdir(source_dir)

        for file_name in file_names:
            # Moves files from source to target
            shutil.move(os.path.join(source_dir, file_name), target_dir)
    except (shutil.Error, OSError) as e:
        log.error("source: %s target: %s Error: %s" % (source_dir, target_dir, e.strerror))
        raise


def create_folders_if_missing(paths):
    for path in paths:
        try:
            # Check whether the specified path exists or not
            is_exist = os.path.exists(path)

            if not is_exist:
                # Create a new directory because it does not exist
                os.makedirs(path)
                log.info("The new directory %s is created!" % path)
        except OSError as e:
            log.error("path: %s Error: %s" % (path, e.strerror))
            raise
