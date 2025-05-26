from dependency_injector import containers, providers


class Movie:
    def __init__(self, title: str, year: int, director: str):
        self.title = str(title)
        self.year = int(year)
        self.director = str(director)

    def __repr__(self):
        return "{0}(title={1}, year={2}, director={3})".format(
            self.__class__.__name__,
            repr(self.title),
            repr(self.year),
            repr(self.director)
        )


import csv
from typing import Callable, List


class MovieFinder:
    def __init__(self, movie_factory: Callable[..., Movie]) -> None:
        self._movie_factory = movie_factory

    def find_all(self) -> List[Movie]:
        raise NotImplementedError()


class CsvMovieFinder(MovieFinder):
    def __init__(self, movie_factory: Callable[..., Movie], path: str, delimiter: str) -> None:
        self._csv_file_path = path
        self._delimiter = delimiter
        super().__init__(movie_factory)

    def find_all(self) -> List[Movie]:
        with open(self._csv_file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=self._delimiter)
            return [self._movie_factory(*row) for row in csv_reader]


class MovieLister:
    def __init__(self, movie_finder: MovieFinder):
        self._movie_finder = movie_finder

    def movies_directed_by(self, director):
        return [
            movie for movie in self._movie_finder.find_all()
            if movie.director == director
        ]

    def movies_released_in(self, year):
        return [
            movie for movie in self._movie_finder.find_all()
            if movie.year == year
        ]


class Container(containers.DeclarativeContainer):
    config = providers.Configuration(yaml_files=["config.yml"])
    movie = providers.Factory(Movie)
    csv_finder = providers.Singleton(
        CsvMovieFinder,
        movie_factory=movie.provider,
        path=config.finder.csv_path,
        delimiter=config.finder.csv.delimiter
    )

    lister = providers.Factory(
        MovieLister,
        movie_finder=csv_finder
    )