import logging
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

import movies
import util

log = logging.getLogger(__name__)


def get_category_urls(genre_url):
    data = {}
    try:
        # Make a GET request to fetch the raw HTML content
        html_content = requests.get(genre_url).text
    except requests.exceptions.RequestException as e:
        log.error("url: %s Error: %s" % (genre_url, e.strerror))
        raise

    genre_url_array = urlparse(genre_url)
    soup = BeautifulSoup(html_content, "lxml")
    div = soup.select("a[name=slot_right-4] + div")

    # get the category urls for the top 50 list
    for a in div[0].find_all('a', href=True):
        data[a.text.strip()] = "".join(["https://", genre_url_array.netloc, a['href']])
    return data


def get_movie_info(top_category, url, timestamp):
    data = []
    try:
        # Make a GET request to fetch the raw HTML content
        html_content = requests.get(url).text
    except requests.exceptions.RequestException as e:
        log.error("url: %s Error: %s" % (url, e.strerror))
        raise

    soup = BeautifulSoup(html_content, "lxml")
    movie_list = soup.select("div .lister-item-content")

    # Iterate through each movie section
    for movie in movie_list:
        # dict to hold movie info
        movie_info = {}
        # selecting header element
        header = movie.find('h3', class_="lister-item-header")

        # rest of function is web scraping element by html tag and css selector
        movie_info["title"] = header.find('a').text
        movie_info["url"] = header.find('a')['href']
        # remove period after rank
        movie_info["imdb_rank"] = {top_category: header.find('span', class_="lister-item-index").text.replace(".", "")}
        movie_info["release_year"] = header.find('span', class_="lister-item-year").text

        mpaa_rating = movie.find('span', class_="certificate")
        # setting default case to None
        movie_info["mpaa_rating"] = None
        if mpaa_rating is not None:
            movie_info["mpaa_rating"] = mpaa_rating.text

        runtime_minutes = movie.find('span', class_="runtime")
        # setting default case to None
        movie_info["runtime_minutes"] = None
        if runtime_minutes is not None:
            movie_info["runtime_minutes"] = runtime_minutes.text.strip()

        genres = movie.find('span', class_="genre")
        # setting default case to None
        movie_info["genres"] = None
        if genres is not None:
            sorted_genre_list = sorted(genres.text.strip().split(", "))
            movie_info["genres"] = sorted_genre_list

        nv_element = movie.select('span[name=nv]')
        # setting default case to None
        movie_info["num_votes"] = None
        # setting default case to None
        movie_info["gross_earnings"] = None
        if nv_element:
            movie_info["num_votes"] = nv_element[0]['data-value']
            if len(nv_element) > 1:
                movie_info["gross_earnings"] = nv_element[1]['data-value']

        bottom_row = movie.select('p:-soup-contains("Director")')

        directors_and_actors = bottom_row[0].select("p > a")
        actors = bottom_row[0].select("span ~ a")

        # finding the directors by removing actors from list
        directors = list(set(directors_and_actors) - set(actors))

        # sorting list to help keep order the same for separate extracts
        sorted_actor_list = sorted([actor.text for actor in actors])
        sorted_director_list = sorted([director.text for director in directors])

        movie_info["actors"] = sorted_actor_list
        movie_info["directors"] = sorted_director_list

        summary = movie.select('div.ratings-bar + p')
        movie_info["summary"] = summary[0].text.strip()

        imdb_rating = movie.find('div', class_="ratings-imdb-rating")
        # setting default case to None
        movie_info["imdb_rating"] = None
        if imdb_rating is not None:
            movie_info["imdb_rating"] = imdb_rating['data-value']

        metascore_rating = movie.find('span', class_="metascore")
        # setting default case to None
        movie_info["metascore_rating"] = None
        if metascore_rating is not None:
            movie_info["metascore_rating"] = metascore_rating.text.strip()

        # appending to movie_info list if not None
        if movie_info is not None:
            data.append(movies.Movie(movie_info, timestamp))

    return data


def export_archived_file(genre_url, path, archive_path):
    # create folders for files
    util.create_folders_if_missing([path, archive_path])
    # archiving old files
    util.archive_old_files(path, archive_path)
    # combine all category data into a single dataframe
    movies_list = []
    categories = get_category_urls(genre_url)
    timestamp = datetime.utcnow()
    for k, v in categories.items():
        movies_list = movies_list + get_movie_info(k, v, timestamp)

    # write a pandas dataframe to gzipped CSV file
    movies_df = pd.DataFrame.from_records([m.to_dict() for m in movies_list])
    file_created_time = datetime.now().strftime("%Y%m%d-%H%M%S")
    file_name = f"{path}/top50movies{file_created_time}.csv.gz"

    try:
        movies_df.to_csv(file_name, index=False, compression='gzip')
        log.info("zipped file created")
    except BaseException as e:
        log.error("df: %s Error: %s" % (movies_df, e))


