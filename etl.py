from doctest import UnexpectedException
import os
import glob
import psycopg2
from psycopg2.errors import UniqueViolation
import pandas as pd
from sql_queries import *
from datetime import datetime


def clean_data(df, na_subset, dup_subset=None):
    """This function cleans the dataframe with following the Normalization rules:
    1. Data Integrity by droping duplicates on ID.
    2. There are some '' ids those are turned to NaN and dropped for ids through droping_duplicates.
    3. Drop na on ids
    The imputations on the length is not done. If required can be added here.
    Args:
        [df] -> It is the dataframe object under consideration
        [na_subset] -> It is the list of columns where na should be removed
        [dup_set] -> It is the list of columns wherer the duplicates should not be considered. Ideally the id columns
        returns cleaned dataframe
    """
    # Common cleaning to replace '' with NaN on ids
    temp_df = df.copy()
    temp_df.dropna(subset=na_subset, inplace=True)
    temp_df.drop_duplicates(subset=dup_subset, inplace=True)
    return temp_df


def write_duplicate_records(data):
    """
    Writing the duplicate records to a file to keep track of ids which are of duplication.
    Args:
        (data)(List): List of data.
    """
    with open(f"duplicate_records.txt", "a+") as f:
        f.write(f"[INFO][{datetime.utcnow()}] - {','.join(str(item) for item in data)} \n")


def process_song_file(cur, filepath):
    """Process the song file and insert into to database at artist and song table.
    Note: We need to insert first to the artist table and then to song table avoiding
    foreign key violation.
    Args:
        cur (_type_): The connection cursor object
        filepath (str): The filepath of the song data
    """
    try:
        # open song file
        df = pd.read_json(filepath, lines=True)

        # insert artist record
        artist_data = df[
            [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude",
            ]
        ]
        insertion_data = artist_data.values.tolist()[0]
        cur.execute(artist_table_insert, insertion_data)

        # insert song record
        song_data = df[["song_id", "title", "artist_id", "year", "duration"]]
        insertion_data = song_data.values.tolist()[0]
        cur.execute(song_table_insert, insertion_data)
    except UniqueViolation as uexc:
        print(uexc)
        print("The data has already an entry in the db")
        print(f"Data - {insertion_data}")
        write_duplicate_records(insertion_data)


def process_log_file(cur, filepath):
    """This function process the log data file and writes to timestamp table,
    user table and songplay finally.

    Args:
        cur (_type_): _description_
        filepath (str): The log data file.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = df.copy()
    t["ts"] = t["ts"].apply(lambda x: pd.to_datetime(x, unit="ms"))
    t["hour"] = t["ts"].apply(lambda x: x.hour)
    t["day"] = t["ts"].apply(lambda x: x.day)
    t["week"] = t["ts"].apply(lambda x: x.week)
    t["month"] = t["ts"].apply(lambda x: x.month)
    t["year"] = t["ts"].apply(lambda x: x.year)
    t["weekday"] = t["ts"].apply(lambda x: x.dayofweek)
    time_df = t[["ts", "hour", "day", "week", "month", "year", "weekday"]]
    time_df = clean_data(time_df, na_subset=["ts"], dup_subset=["ts"])

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except UniqueViolation as uxc:
            print(uxc)
            print(list(row))
            write_duplicate_records(row)
            continue
        except Exception as e:
            print(e)
            continue

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    user_df = clean_data(user_df, na_subset=["userId"], dup_subset=["userId"])
    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except UniqueViolation as uxc:
            print(uxc)
            print(list(row))
            write_duplicate_records(row)
            continue
        except Exception as e:
            print(e)
            continue
    # insert songplay records
    for index, row in df.iterrows():
        try:
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()

            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            # insert songplay record
            songplay_data = (
                pd.to_datetime(row.ts, unit="ms"),
                row.userId,
                row.level,
                songid,
                artistid,
                row.sessionId,
                row.location,
                row.userAgent,
            )
            cur.execute(songplay_table_insert, songplay_data)
        except Exception as e:
            print(e)
            print(list(row))
            continue


def process_data(cur, conn, filepath, func):
    """The function iterates to the provided data path in-depth meaning
    it will search through the sub directories and obtain the absolute path
    from that using glob. Each of such locations are added into a global list
    The global list is iterated to perform processing of function (func) parameter.
    Args:
        cur (object): The postgres connection cursor
        conn (object): The postgres connection
        filepath (str): The data path. In our case the data/song_data or data/log_data
        func (object): The function object reference itself is provided and invoked during the processing.
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    """
    The main wrapper function which wraps the entire pipeline.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    # Avoiding not being saved while there is an exception.
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
