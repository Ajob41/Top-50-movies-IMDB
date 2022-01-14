

class Movie(object):
    def __init__(self, movie_obj, datetime):
        self.title = movie_obj["title"]
        self.url = movie_obj["url"]
        self.imdb_rank = movie_obj["imdb_rank"]
        self.release_year = movie_obj["release_year"]
        self.mpaa_rating = movie_obj["mpaa_rating"]
        self.runtime_minutes = movie_obj["runtime_minutes"]
        self.genres = movie_obj["genres"]
        self.imdb_rating = movie_obj["imdb_rating"]
        self.metascore_rating = movie_obj["metascore_rating"]
        self.actors = movie_obj["actors"]
        self.directors = movie_obj["directors"]
        self.summary = movie_obj["summary"]
        self.num_votes = movie_obj["num_votes"]
        self.gross_earnings = movie_obj["gross_earnings"]
        self.timestamp = datetime

    def to_dict(self):
        return self.__dict__
