import json

class Memoize:
    """ Adapted from
        https://stackoverflow.com/questions/1988804/what-is-memoization-and-how-can-i-use-it-in-python"""
    def __init__(self, f):
        self.f = f
        self.memo = {}
    def __call__(self, *args):
        if not args in self.memo:
            self.memo[args] = self.f(*args)
        #Warning: You may wish to do a deepcopy here if returning objects
        return self.memo[args]

@Memoize
def load_movies(json_path):
    return json.load(open(json_path))

def get_movie(tmdb_id, movies="data/tmdb.json"):
    movies = load_movies(movies)
    tmdb_id=str(tmdb_id)
    return movies[tmdb_id]

def noop(src_movie, base_doc):
    return base_doc


def indexable_movies(enrich=noop, movies="data/tmdb.json"):
    """ Generates TMDB movies, similar to how ES Bulk indexing
    uses a generator to generate bulk index/update actions"""
    movies = load_movies(movies)
    for movieId, tmdbMovie in movies.items():
        try:
            releaseDate = None
            if "release_date" in tmdbMovie and len(tmdbMovie["release_date"]) > 0:
                releaseDate = tmdbMovie["release_date"]
                releaseYear = releaseDate[0:4]

            full_poster_path = ""
            if "poster_path" in tmdbMovie and tmdbMovie["poster_path"] is not None and len(tmdbMovie["poster_path"]) > 0:
                full_poster_path = "https://image.tmdb.org/t/p/w185" + tmdbMovie["poster_path"]

            base_doc = {"id": movieId,
                        "title": tmdbMovie["title"],
                        "overview": tmdbMovie["overview"],
                        "tagline": tmdbMovie["tagline"],
                        "directors": [director["name"] for director in tmdbMovie["directors"]],
                        "cast": " ".join([castMember["name"] for castMember in tmdbMovie["cast"]]),
                        "genres": [genre["name"] for genre in tmdbMovie["genres"]],
                        "release_date": releaseDate,
                        "release_year": releaseYear,
                        "poster_path": full_poster_path,
                        "vote_average": float(tmdbMovie["vote_average"]) if "vote_average" in tmdbMovie else None,
                        "vote_count": int(tmdbMovie["vote_count"]) if "vote_count" in tmdbMovie else 0,
                      }
            yield enrich(tmdbMovie, base_doc)
        except KeyError as k: # Ignore any movies missing these attributes
            continue
