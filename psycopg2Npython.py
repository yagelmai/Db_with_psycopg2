from typing import List, Tuple, Any

# from psycopg2 import sql
import psycopg2
import csv
import os
import glob
import time
import pandas


import Utility.DBConnector as Connector
# from Business.Actor import Actor
# from Business.Critic import Critic
# from Business.Movie import Movie
# from Business.Studio import Studio
from Utility.Exceptions import DatabaseException
from Utility.ReturnValue import ReturnValue

# ---------------------------------- CRUD API: ----------------------------------

def createCsvTables():
    conn = None


    try:
        conn = Connector.DBConnector()

        csvPath = "../out"


        for filename in glob.glob(os.path.join(csvPath,"*.csv")):
        # Create a table name
            tablename = filename.replace("../out", "").replace(".csv", "")[1:-6] + "_csv"
            print(tablename)

            # Open file
            fileInput = open(filename, "r")

            # Extract first line of file
            firstLine = fileInput.readline().strip()

            # Split columns into an array [...]
            columns = firstLine.split(",")

            # Build SQL code to drop table if exists and create table
            sqlQueryCreate = 'DROP TABLE IF EXISTS '+ tablename + ";\n"
            sqlQueryCreate += 'CREATE TABLE '+ tablename + "("

            #some loop or function according to your requiremennt
            # Define columns for table
            for column in columns:
                sqlQueryCreate += column + " TEXT,\n"

            sqlQueryCreate = sqlQueryCreate[:-2]
            sqlQueryCreate += ");"

            conn.execute(sqlQueryCreate)
            conn.commit()

        #print tables
        _, entries = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
        for table in entries.rows:
            print("table name created in postgres: ")
            print(table)
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()

def fix_list(lst):
    # create a dictionary to store the counts of each element
    element_counts = {}
    fixed_list = []
    for element in lst:
        if element in element_counts:
            element_counts[element] += 1
            fixed_list.append(element + str(element_counts[element]))
        else:
            element_counts[element] = 1
            fixed_list.append(element)
    return fixed_list


def createMapfileTables():
    conn = None

    try:
        conn = Connector.DBConnector()

        csvPath = "../out"

        for filename in glob.glob(os.path.join(csvPath, "*.mapfile")):
            # Create a table name
            tablename = filename.replace("../out", "").replace(
                ".mapfile", "")[1:-4] + "_mapfile"
            print(tablename)

            # Open file
            fileInput = open(filename, "r")

            # Extract first line of file
            firstLine = fileInput.readline().strip()

            # Split columns into an array [...]
            columns = firstLine.split(",")

            # Build SQL code to drop table if exists and create table
            sqlQueryCreate = 'DROP TABLE IF EXISTS ' + tablename + ";\n"
            sqlQueryCreate += 'CREATE TABLE ' + tablename + "("

            # some loop or function according to your requiremennt
            # Define columns for table
            columns = fix_list(columns)
            for column in columns:
                sqlQueryCreate += column + " TEXT,\n"

            sqlQueryCreate = sqlQueryCreate[:-2]
            sqlQueryCreate += ");"

            conn.execute(sqlQueryCreate)
            conn.commit()

        # print tables
        _, entries = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
        for table in entries.rows:
            print("table name created in postgres: ")
            print(table)
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()

def dropPower():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS POWER CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()

def droprtl():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS RTL CASCADE")
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


def dropTables():
    dropPower()
    droprtl()


def catchException(e: Exception, conn: Any) -> ReturnValue:
    try:
        raise e
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

def copyFromCsvToTable():
    conn = None
    try:
        conn = Connector.DBConnector()
        # Use the COPY statement to copy the contents of the CSV file into the table

        with open('../out/par_exe.power.csv', 'r') as f:
            conn.cursor.copy_expert("COPY par_exe_csv FROM STDIN WITH (FORMAT CSV)", f)
        # Commit the transaction
        conn.commit()
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()

def copyFromMapfileToTable():
    conn = None
    try:
        conn = Connector.DBConnector()
        # Use the COPY statement to copy the contents of the CSV file into the table

        with open('../out/par_exe.rtl.mapfile', 'r') as f:
            conn.cursor.copy_expert("COPY par_exe_mapfile FROM STDIN WITH (FORMAT CSV)", f)
        # Commit the transaction
        conn.commit()
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()

def joinCsvAndMapf():
    conn = None
    try:
        conn = Connector.DBConnector()
        # Use the COPY statement to copy the contents of the CSV file into the table
        query = psycopg2.sql.SQL(
        "CREATE TABLE csvJoinMap AS "
        "SELECT par_exe_csv.*, par_exe_mapfile.* "
        "FROM par_exe_csv "
        "INNER JOIN par_exe_mapfile "
        "ON (par_exe_csv.cell_name LIKE '%' || par_exe_mapfile.dlvrloadgndvsense0|| '%') "
        "LIMIT 100")
        conn.execute(query)

        # Commit the transaction
        conn.commit()
    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()

#
# def clearTables():
#     # TODO: implement
#     pass
#
#
# def dropCritics():
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("DROP TABLE IF EXISTS Critics CASCADE")
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#
#
# def dropActors():
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("DROP TABLE IF EXISTS Actors CASCADE")
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#
#
# def dropMovies():
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("DROP TABLE IF EXISTS Movies CASCADE")
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#
#
# def dropStudios():
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("DROP TABLE IF EXISTS Studios CASCADE")
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#
#
# def dropCriticsMovie():
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("DROP TABLE IF EXISTS CriticsMovie CASCADE")
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#
#
# def dropActorsMovie():
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("DROP TABLE IF EXISTS ActorsMovie CASCADE")
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#
#
# def dropStudiosMovie():
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("DROP TABLE IF EXISTS StudiosMovie CASCADE")
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()


#
# def addCritic(critic: Critic) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL("INSERT INTO Critics(id, name) VALUES({id}, {username})").format(
#             id=sql.Literal(critic.getCriticID()), username=sql.Literal(critic.getName())))
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def addActor(actor: Actor) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL("INSERT INTO Actors(id, name, age, height) VALUES("
#                     "{id}, {name}, {age}, {height})").format(
#             id=sql.Literal(actor.getActorID()),
#             name=sql.Literal(actor.getActorName()),
#             age=sql.Literal(actor.getAge()),
#             height=sql.Literal(actor.getHeight())))
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def addStudio(studio: Studio) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL("INSERT INTO Studios(id, name) VALUES({id}, {name})").format(
#             id=sql.Literal(studio.getStudioID()),
#             name=sql.Literal(studio.getStudioName())))
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def deleteCritic(critic_id: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(sql.SQL("DELETE FROM Critics WHERE id={c_id}".format(c_id=critic_id)))
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def getCriticProfile(critic_id: int) -> Critic:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         _, (name) = conn.execute(sql.SQL(f"SELECT name FROM Critics WHERE id={critic_id}"))
#         return Critic(critic_id, name)
#     except Exception as e:
#         catchException(e, conn)
#         return None
#
#
# def deleteActor(actor_id: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(sql.SQL(f"DELETE FROM Actors WHERE id={sql.Literal(actor_id)}"))
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def getActorProfile(actor_id: int) -> Actor:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         _, (name, age, height) = conn.execute(sql.SQL(f"SELECT name, age, height FROM Critics WHERE id={actor_id}"))
#         return Actor(actor_id, name, age, height)
#     except Exception as e:
#         catchException(e, conn)
#         return None
#
#
# def addMovie(movie: Movie) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL("INSERT INTO Movies(name, year, genere) VALUES({name}, {year}, {genere})").format(
#             name=sql.Literal(movie.getMovieName()), year=sql.Literal(movie.getYear()),
#             genere=sql.Literal(movie.getGenre())))
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK




#
# def deleteMovie(movie_name: str, year: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL(f"DELETE FROM Movies WHERE name={sql.Literal(movie_name)} AND year={sql.Literal(year)}"))
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def getMovieProfile(movie_name: str, year: int) -> Movie:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         _, (genere) = conn.execute(sql.SQL(f"SELECT genere FROM Critics WHERE name={movie_name} AND year={year}"))
#         return Movie(movie_name, year, genere)
#     except Exception as e:
#         catchException(e, conn)
#         return None
#
#
# def deleteStudio(studio_id: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL(f"DELETE FROM Movies WHERE if={sql.Literal(studio_id)}"))
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def getStudioProfile(studio_id: int) -> Studio:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         _, (name) = conn.execute(sql.SQL(f"SELECT name FROM Studios WHERE id={studio_id}"))
#         return Studio(studio_id, name)
#     except Exception as e:
#         catchException(e, conn)
#         return None
#
#
# def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL("INSERT INTO CriticsMovie(critic_id, movie_name, movie_year, rating) VALUES("
#             "{id}, {name}, {year}, {rating})").format(
#             id=sql.Literal(criticID), name=sql.Literal(movieName),
#             year=sql.Literal(movieYear), rating=sql.Literal(rating)))
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL(f"DELETE FROM CriticsMovie WHERE movie_name={sql.Literal(movieName)} AND "
#             f"movie_year={sql.Literal(movieYear)} AND critic_id={criticID}"))
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL("INSERT INTO CriticsMovie(actor_id, movie_name, movie_year, salary, roles) VALUES("
#             "{id}, {name}, {year}, {salary}, {roles})").format(
#             id=sql.Literal(actorID), name=sql.Literal(movieName), year=sql.Literal(movieYear),
#             rating=sql.Literal(salary), roles=sql.Literal(roles)))
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL(f"DELETE FROM ActoresMovie WHERE movie_name={sql.Literal(movieName)} AND "
#             f"movie_year={sql.Literal(movieYear)} AND actor_id={actorID}"))
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
# def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         conn.execute(
#             sql.SQL("INSERT INTO StudiosMovie(studio_id, movie_name, movie_year, budget, revenue) VALUES("
#             "{id}, {name}, {year}, {budget}, {revenue})").format(
#             id=sql.Literal(studioID), name=sql.Literal(movieName), year=sql.Literal(movieYear),
#             budget=sql.Literal(budget), revenue=sql.Literal(revenue)))
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         quer = "DELETE FROM StudiosMovie WHERE (movie_name='{m_name}' and movie_year={m_year} and studio_id={s_id})"\
#             .format(m_name=str(movieName), m_year=movieYear, s_id=studioID)
#         conn.execute(sql.SQL(quer))
#
#     except Exception as e:
#         catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# # ---------------------------------- BASIC API: ----------------------------------
# def averageRating(movieName: str, movieYear: int) -> float:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         quer = "SELECT avg(rating) FROM criticsmovie WHERE movie_name='{m_name}' AND movie_year={m_year}"\
#             .format(m_name=movieName, m_year=movieYear)
#         _, (aver) = conn.execute(sql.SQL(quer))
#
#         # select avg(rating), year, name from
#         # movies inner join criticsmovie
#         # ON (year=movie_year and name=movie_name)
#         # group by year, name
#
#
#         return aver
#     except Exception as e:
#         catchException(e, conn)
#         return None
#
#
# def averageActorRating(actorID: int) -> float:
#     conn = None
#     try:
#         conn = Connector.DBConnector()
#         quer = "select avg(rating) from actorsmovie join criticsmovie " \
#                "ON (actorsmovie.movie_year=criticsmovie.movie_year " \
#                "and actorsmovie.movie_name=criticsmovie.movie_name) " \
#                "group by criticsmovie.movie_year, criticsmovie.movie_name, actor_id " \
#                "HAVING actor_id={a_id}" \
#             .format(a_id=actorID)
#         _, (aver) = conn.execute(sql.SQL(quer))
#
#         return aver
#     except Exception as e:
#         catchException(e, conn)
#         return None
#
#
#
# def getMovies(printSchema: bool = False):
#     conn = None
#     Connector.ResultSet()
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("SELECT * FROM Movies", printSchema=printSchema)
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def getActors(printSchema: bool = False):
#     conn = None
#     Connector.ResultSet()
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("SELECT * FROM Actors", printSchema=printSchema)
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def getStudios(printSchema: bool = False):
#     conn = None
#     Connector.ResultSet()
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("SELECT * FROM Studios", printSchema=printSchema)
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#
#
# def getCritics(printSchema: bool = False):
#     conn = None
#     Connector.ResultSet()
#     try:
#         conn = Connector.DBConnector()
#         conn.execute("SELECT * FROM Critics", printSchema=printSchema)
#     except Exception as e:
#         return catchException(e, conn)
#     if conn is not None:
#         conn.close()
#     return ReturnValue.OK
#

# GOOD LUCK!

def getTableAsDF():
    conn = None
    try:
        conn = Connector.DBConnector()
        # Use the COPY statement to copy the contents of the CSV file into the table
        query = psycopg2.sql.SQL(
            "SELECT * FROM csvjoinmap LIMIT 11"
        )
        _, res = conn.execute(query)
        df = pandas.DataFrame(res.rows, res.cols_header[:-1])
        return df
        # Commit the transaction
        conn.commit()

    except Exception as e:
        catchException(e, conn)
    if conn is not None:
        conn.close()


if __name__ == '__main__':
    f_time = time.time()
    print("start!")
    # dropTables()
    # createCsvTables()
    # createMapfileTables()
    # copyFromCsvToTable()
    # copyFromMapfileToTable()
    # joinCsvAndMapf()
    df = getTableAsDF()
    print(df)
    l_time = time.time()
    print("successful finished! at time: ")
    print(l_time - f_time)
