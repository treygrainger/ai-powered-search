import json
from pyspark.sql import SparkSession, Row

def load_dataframe(movie_file="data/tmdb.json"):
    movies = []
    for movieId, tmdbMovie in json.load(open(movie_file)).items():
        try:
            releaseDate = None
            if "release_date" in tmdbMovie and len(tmdbMovie["release_date"]) > 0:
                releaseDate = tmdbMovie["release_date"]
                releaseYear = releaseDate[0:4]

            full_poster_path = ""
            if "poster_path" in tmdbMovie and tmdbMovie["poster_path"] is not None and len(tmdbMovie["poster_path"]) > 0:
                full_poster_path = "https://image.tmdb.org/t/p/w185" + tmdbMovie["poster_path"]

            movie = {"id": movieId,
                    "title": tmdbMovie["title"],
                    "overview": tmdbMovie["overview"],
                    "tagline": tmdbMovie["tagline"],
                    "directors": [director["name"] for director in tmdbMovie["directors"]],
                    "cast": " ".join([castMember["name"] for castMember in tmdbMovie["cast"]]),
                    "genres": [genre["name"] for genre in tmdbMovie["genres"]],
                    "release_date": releaseDate,
                    "release_year": releaseYear,
                    "poster_url": tmdbMovie["poster_path"],
                    "poster_path": full_poster_path,
                    "vote_average": float(tmdbMovie["vote_average"]) if "vote_average" in tmdbMovie else None,
                    "vote_count": int(tmdbMovie["vote_count"]) if "vote_count" in tmdbMovie else 0}
            movies.append(movie)
        except KeyError as k: # Ignore any movies missing these attributes
            continue
    spark = SparkSession.builder.appName("AIPS").getOrCreate()
    return spark.createDataFrame(Row(**m) for m in movies)