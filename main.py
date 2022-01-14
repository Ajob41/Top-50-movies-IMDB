import config
import extract
import transform
import load
from log import logging_config


def main():
    # Web scrapes imdb, exports the gzip file to ./raw_data, and archives old files
    new_data_pull = input("Export new top 50 data?: (y or n) ")
    log.info("user input to export top 50: %s" % new_data_pull)
    if new_data_pull == 'y':
        extract.export_archived_file(config.IMDB_GENRE_URL, config.RAW_DATA_PATH, config.RAW_DATA_ARCHIVE_PATH)

    # Transforms the raw csv into typing and format for ingestion into postgres staging table
    update_staging = input("\nUpdate staging table?: (y or n) ")
    log.info("user input to update staging: %s" % update_staging)
    if update_staging == 'y':
        transform.ingest_new_staging_data(config.RAW_DATA_PATH, config.CONNECTION_STRING)

    # Populates the dimension and fact table if theres a delta in the imdb data
    populate_schema = input("\nPopulate schema?: (y or n) ")
    log.info("user input to populate schema: %s" % populate_schema)
    if populate_schema == 'y':
        load.populate_schema(config.CONNECTION_STRING)


if __name__ == '__main__':
    # instantiate logger with config
    log = logging_config.configure_logger("default", "./log/movie_log.log")
    log.info("program start")
    main()
