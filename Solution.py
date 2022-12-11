from typing import List, Tuple

from psycopg2 import sql

import Utility.DBConnector as Connector
from Business.Actor import Actor
from Business.Critic import Critic
from Business.Movie import Movie
from Business.Studio import Studio
from Utility.Exceptions import DatabaseException
from Utility.ReturnValue import ReturnValue


# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        # conn.execute("CREATE TABLE Critics("
        #              "id INTEGER PRIMARY KEY,"
        #              "name TEXT NOT NULL)"
        #              )
        # conn.execute("CREATE TABLE Movies("
        #              "name TEXT,"
        #              "year INTEGER CHECK(year >= 1895),"
        #              "genere TEXT CHECK(genere IN ('Drama', 'Action', 'Comedy', 'Horror')) NOT NULL,"
        #              "PRIMARY KEY (name, year))"
        #              )
        # conn.execute("CREATE TABLE Actors("
        #              "id INTEGER PRIMARY KEY CHECK(id > 0),"
        #              "name TEXT NOT NULL,"
        #              "age INTEGER NOT NULL CHECK(age > 0),"
        #              "height INTEGER NOT NULL CHECK(height > 0))"
        #              )
        # conn.execute("CREATE TABLE Studios("
        #              "id INTEGER PRIMARY KEY,"
        #              "name TEXT NOT NULL)"
        #              )

        # conn.execute("CREATE VIEW Critics_movie("
        #             "SELECT C.id,"
        #            "M.year, M.name"
        #             "FROM Critics as C, Movies as M )"
        #             )
        # conn.execute("CREATE VIEW Studio_movie("
        #             "SELECT S.id, M.year, M.name"
        #             "FROM Studio S, Movies M )"
        #             )
        # conn.execute("CREATE VIEW Actor_movie("
        #             "SELECT A.id, M.year, M.name"
        #             "FROM Actors A, Movies M )"
        #             )
        conn.execute(
            "CREATE TABLE Critics_movie("
            "critic_id INTEGER REFERENCES Critics(id),"
            "movie_name TEXT NOT NULL REFERENCES Movies(name),"
            "movie_year INTEGER REFERENCES Movies(year),"
            "UNIQUE(critic_id),"
            "UNIQUE(movie_name, movie_year))"
        )
        conn.execute(
            "CREATE TABLE Studio_movie("
            "studio_id INTEGER REFERENCES Studio(id),"
            "movie_name TEXT NOT NULL REFERENCES Movies(name),"
            "movie_year INTEGER REFERENCES Movies(year),"            
            "UNIQUE(studio_id),"
            "UNIQUE(movie_name, movie_year))"
        )
        conn.execute(
            "CREATE TABLE Actor_movie("
            "actor_id INTEGER REFERENCES Actors(id),"
            "movie_name TEXT NOT NULL REFERENCES Movies(name),"
            "movie_year INTEGER REFERENCES Movies(year),"
            "UNIQUE(actor_id),"
            "UNIQUE(movie_name, movie_year))"
        )

    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        if conn is not None:
            conn.close()


def clearTables():
    # TODO: implement
    pass


def dropTables():
    # TODO: implement
    pass


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Critics(id, name) VALUES({id}, {username})").format(
            id=sql.Literal(critic.getCriticID()), username=sql.Literal(critic.getName()))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return ReturnValue.OK


def addActor(actor: Actor) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Actors(id, name, age, height) VALUES({id}, {name}, {age}, {height})").format(
            id=sql.Literal(actor.getActorID()),
            name=sql.Literal(actor.getActorName()),
            age=sql.Literal(actor.getAge()),
            height=sql.Literal(actor.getHeight()))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return ReturnValue.OK


def addStudio(studio: Studio) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Studios(id, name) VALUES({id}, {name})").format(
            id=sql.Literal(studio.getStudioID()),
            name=sql.Literal(studio.getStudioName()))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return ReturnValue.OK


def deleteCritic(critic_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getCriticProfile(critic_id: int) -> Critic:
    # TODO: implement
    pass


def deleteActor(actor_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getActorProfile(actor_id: int) -> Actor:
    # TODO: implement
    pass


def addMovie(movie: Movie) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Movies(name, year, genere) VALUES({name}, {year}, {genere})").format(
            name=sql.Literal(movie.getMovieName()), year=sql.Literal(movie.getYear()),
            genere=sql.Literal(movie.getGenre()))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return ReturnValue.OK


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    # TODO: implement
    pass


def getMovieProfile(movie_name: str, year: int) -> Movie:
    # TODO: implement
    pass


def deleteStudio(studio_id: int) -> ReturnValue:
    # TODO: implement
    pass


def getStudioProfile(studio_id: int) -> Studio:
    # TODO: implement
    pass


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    # TODO: implement
    pass


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    # TODO: implement
    pass


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    # TODO: implement
    pass


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    # TODO: implement
    pass


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    # TODO: implement
    pass


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    # TODO: implement
    pass


def averageActorRating(actorID: int) -> float:
    # TODO: implement
    pass


def bestPerformance(actor_id: int) -> Movie:
    # TODO: implement
    pass


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    # TODO: implement
    pass


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def studioRevenueByYear() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


def getFanCritics() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def averageAgeByGenre() -> List[Tuple[str, float]]:
    # TODO: implement
    pass


def getExclusiveActors() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def getMovies(printSchema: bool = False):
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Movies", printSchema=printSchema)
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return result


def getActors(printSchema: bool = False):
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Actors", printSchema=printSchema)
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return result


def getStudios(printSchema: bool = False):
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Studios", printSchema=printSchema)
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return result


def getCritics(printSchema: bool = False):
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Critics", printSchema=printSchema)
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return result


# GOOD LUCK!
if __name__ == '__main__':
    createTables()
    # addCritic(Critic(1, "Moshe"))
    # addCritic(Critic(2, "Yagel"))
    # addCritic(Critic(3, "Avigail"))
    # addMovie(Movie("Best Movie", 2000, 'Action'))
    # addMovie(Movie("Worst Movie", 1990, 'Horror'))
    # addMovie(Movie("Ok Movie", 2005, 'Comedy'))
    # addActor(Actor(1, "Hilbert", 4, 8))
    # addActor(Actor(8, "So-Yang", 16, 5))
    # addActor(Actor(45, "Luna", 70, 6))
    # addActor(Actor(13, "Miley", 13, 9))
    # addActor(Actor(2, "Gon", 34, 1))
    # addStudio(Studio(5, "Baloo"))
    # addStudio(Studio(6, "Shick"))
    print('critics:')
    getCritics(printSchema=True)
    print('movies:')
    getMovies(printSchema=True)
    print('actors:')
    getActors(printSchema=True)
    print('studios:')
    getStudios(printSchema=True)
