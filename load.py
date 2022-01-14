import logging

import util

log = logging.getLogger(__name__)


def get_new_time_dim(conn_str):
    new_time_dim_query = """
        SELECT DISTINCT 
            capture_date,
            month_code,
            quarter_code,
            year
        FROM
            movie_performance_staging
        EXCEPT
        SELECT DISTINCT
            capture_date,
            month_code,
            quarter_code,
            year
        FROM
            day_dim
    """
    return util.read_sql_into_df(conn_str, new_time_dim_query)


def get_new_movie_dim(conn_str):
    new_movie_dim_query = """
        SELECT DISTINCT 
            imdb_id,
            title,
            release_year,
            runtime_minutes,
            mpaa_rating,
            genres,
            summary,
            actors,
            directors
        FROM
            movie_performance_staging
        EXCEPT
        SELECT DISTINCT
            imdb_id,
            title,
            release_year,
            runtime_minutes,
            mpaa_rating,
            genres,
            summary,
            actors,
            directors
        FROM
            movie_dim
    """

    return util.read_sql_into_df(conn_str, new_movie_dim_query)


def create_new_time_dim_values(conn_str):
    new_time_dim = get_new_time_dim(conn_str)

    if new_time_dim.empty:
        log.warning("No data added to day_dim")
    else:
        util.ingest_df_into_sql(new_time_dim, conn_str, "day_dim", "append")
        log.info("Added data to day_dim")


def create_new_movie_dim_values(conn_str):
    new_movie_dim = get_new_movie_dim(conn_str)

    if new_movie_dim.empty:
        log.warning("No data added to movie_dim")
    else:
        util.ingest_df_into_sql(new_movie_dim, conn_str, "movie_dim", "append")
        log.info("Added data to movie_dim")


def populate_day_key_staging_table(conn_str):
    sql_statement = """
    UPDATE
        movie_performance_staging
    SET
        day_key = day_dim.day_key
    FROM
        day_dim
    WHERE
        movie_performance_staging.capture_date = day_dim.capture_date
    """

    util.run_crud_operation(conn_str, sql_statement)

    log.info("Populated day_key in staging")


def populate_movie_key_staging_table(conn_str):
    sql_statement = """
    UPDATE
        movie_performance_staging
    SET
        movie_key = movie_dim.movie_key
    FROM
        movie_dim
    WHERE
        movie_performance_staging.imdb_id = movie_dim.imdb_id
    """

    util.run_crud_operation(conn_str, sql_statement)

    log.info("Populated movie_key in staging")


def get_movie_key_for_delta(conn_str):
    delta_query = """
                SELECT DISTINCT 
                    movie_key,
                    imdb_rank,
                    gross_earnings,
                    imdb_rating,
                    metascore_rating,
                    num_votes
                FROM
                    movie_performance_staging
                EXCEPT
                SELECT DISTINCT 
                    movie_key,
                    imdb_rank,
                    gross_earnings,
                    imdb_rating,
                    metascore_rating,
                    num_votes
                FROM
                    movie_performance_fact
            """
    delta_df = util.read_sql_into_df(conn_str, delta_query)
    # read as float so converting to int
    movie_key_int_list = delta_df["movie_key"].astype(int).to_list()
    # returning a string list for use in query
    return [str(m) for m in movie_key_int_list]


def populate_movie_fact_table(conn_str, movie_keys):
    sql_query = """
        SELECT DISTINCT 
            day_key,
            movie_key,
            imdb_rank,
            gross_earnings,
            imdb_rating,
            metascore_rating,
            num_votes,
            file_name,
            timestamp
        FROM
            movie_performance_staging
        {0}
        """
    # adds where clause to include only delta
    formatted_sql_query = sql_query.format("WHERE movie_key IN (" + ",".join(movie_keys) + ")")
    # read into df and ingest into table
    populated_fact_staging_df = util.read_sql_into_df(conn_str, formatted_sql_query)
    util.ingest_df_into_sql(populated_fact_staging_df, conn_str, "movie_performance_fact", "append")
    log.info("Movie performance fact table populated")


def populate_schema(conn_str):
    # create new movie dimensions if there is any new movies
    create_new_movie_dim_values(conn_str)
    # populate staging table with movie_key
    populate_movie_key_staging_table(conn_str)
    # check if the movie data has changed
    movie_key_list = get_movie_key_for_delta(conn_str)
    # if changed
    if movie_key_list:
        # add time dimensions if theres any new days
        create_new_time_dim_values(conn_str)
        # populate staging table with the day_key
        populate_day_key_staging_table(conn_str)
        # add delta from staging to fact table
        populate_movie_fact_table(conn_str, movie_key_list)
    # if no change do not add anything to fact table
    else:
        log.warning("No new facts populated")
