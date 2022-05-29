import csv
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import Error
import pandas as pd
import json
from tqdm import tqdm

import requests
import os

# Pandas DB connection to MySQL


# curl -X POST 'https://www.picpurify.com/analyse/1.1' -F 'API_KEY=XXX' -F 'task=porn_moderation,drug_moderation,gore_moderation' -F 'origin_id=xxxxxxxxx' -F 'reference_id=yyyyyyyy' -F 'file_image=@/path/to/local/file.jpg'


picpurify_api_keys = ['2pc49N3A92kSBsLPSNqsBfmxBt7Xugzq', 'vpBHzQ0VSOf6kpYwR00VQQAkgwVGLxAN',
                      'zkSwmR6YDbycNicklZ353I47wjwC0N8g'
                      'wnvPrydRKEBUJr4QM5WxbPQLjvxoBNxY',
                      '7tqEl5dizD3t8uPwInzwShLix7j2ZhJ8',
                      'joMQ7up6tf8coc1SJjhGxaWxe2DhMqmq',
                      'P59CqTAgH5ZEmjJZ6Ap4chNwsWYwY3L8',
                      'PUevUac4V6RS88WVzfmR2ev9vQGAoLo8',
                      'PYPPyoVX24R6BL08ibYdSBP0Y1QmkuQp']

api_key_index = 0

picpurify_url = 'https://www.picpurify.com/analyse/1.1'


def fill_data_user(cursor):
    sql_baseQuery = """INSERT INTO users_pre_processed VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    # depending on the size of the table needs to change
    cursor = connection.cursor()
    cursor.execute(
        "SELECT DISTINCT author_id FROM tweets WHERE author_id NOT IN (SELECT id from users_pre_processed);")
    record = cursor.fetchall()
    data = [(str(row).strip('(),'), str(row).strip('(),'), "-1", "-1", "-1", "-1", "-1") for row in record]
    cursor.executemany(sql_baseQuery, data)
    connection.commit()


def sendQuery(cursor, query_string, data):
    cursor = connection.cursor()
    cursor.execute(
        "SELECT author_id FROM tweets WHERE author_id NOT IN (SELECT id from users_pre_processed);")
    record = cursor.fetchall()
    cursor.executemany(query_string, data)
    connection.commit()


def read_name_files(file):
    return pd.read_csv(file, dtype=str, na_values=["?", "NaN", "nan", "NAN"], na_filter=True)


def get_name_from_username(db_connection):
    users = pd.read_sql_query("SELECT id, username, name FROM users;", db_connection)
    user_preprocessed_names = []
    # print(user_preprocessed_names)
    # print(users)
    for index, row in tqdm(users.iterrows()):
        row_name = row["name"].split(" ")[0]
        # print(row_name)


def get_classified_names():
    names_data_us = read_name_files("name_gender_dataset.csv").drop_duplicates("Name")
    # names_data_global = read_name_files("wgnd_ctry.csv").dropna(subset=["gender"])

    name_gender_df_us = pd.DataFrame(
        {"Name": names_data_us["Name"].str.lower(), "Gender": names_data_us["Gender"]})
    # name_gender_df_global = pd.DataFrame(
    #     {"Name": names_data_global["name"].str.lower().str.replace(" ", ""),
    #      "Gender": names_data_global["gender"]})

    # all_names_gender_df = pd.concat([name_gender_df_global, name_gender_df_us]).drop_duplicates()
    return name_gender_df_us


def get_users(db_connection):
    return pd.read_sql_query(
        "SELECT DISTINCT users.id, users.username, users.name, users.profile_picture_url FROM users INNER JOIN tweets t on users.id = t.author_id WHERE (SELECT user_gender FROM data_preprocessed WHERE data_preprocessed.tweet_id = t.id) IS NULL OR (SELECT user_gender FROM data_preprocessed WHERE data_preprocessed.tweet_id = t.id) = -1;",
        db_connection)


def identify_gender(db_connection):
    # TODO: maybe expand list
    not_names = ["the", "and", "stars", "democrat", "republican", "rep", "dem"]

    users_df = get_users(db_connection)
    gendered_names = dict(zip(get_classified_names()['Name'], get_classified_names()["Gender"]))
    preprocessed_users = pd.read_sql_query("SELECT user_id, gender FROM users_pre_processed", db_connection)
    # print(preprocessed_users)

    indexes = []
    result = {}
    data = []

    api_calls_count = 0
    gender_code = -1
    count = 1
    for index, user in tqdm(users_df.iterrows(), total=len(users_df.index), desc="Check tweet author"):
        count += 1
        names = user["name"].split(" ")
        for name in names:
            name = name.lower()
            if name in not_names:
                continue
            if len(name) < 3:
                continue
            if name in gendered_names:
                # print(users_df.iloc[index]["id"])
                result[user["username"]] = gendered_names[name]
                indexes.append(users_df.iloc[index]["id"])
                if result[user["username"]] == "M":
                    gender_code = "1"
                else:
                    gender_code = "2"
                data.append((gender_code, int(users_df.iloc[index]["id"])))
                # print("Identified by Name")
            elif preprocessed_users.iloc[index]["gender"] == -1:
                url = user["profile_picture_url"]
                if url == "null":
                    continue
                query_string = "UPDATE users_pre_processed SET gender=%s WHERE id=%s"

                data.append((classify_gender_by_pic(url), int(users_df.iloc[index]["id"])))
                api_calls_count += 1
                # print("Identified by Picture")
            else:
                # print("Cant identify")
                data.append(("0", int(users_df.iloc[index]["id"])))

        # print("Processed {} of {}".format(count, len(users_df)))
    query_string = "UPDATE users_pre_processed SET gender=%s WHERE id=%s AND gender < 0"
    sendQuery(cursor, query_string, data)
    # df = pd.DataFrame({"id": indexes, "gender": result.values()})

    # print(api_calls_count)
    # print(data)
    return result


def classify_gender_by_pic(picture_url):
    global api_key_index
    try:
        response_data = requests.post(picpurify_url, data={
            "url_image": picture_url,
            "API_KEY": picpurify_api_keys[api_key_index], "task": "face_gender_detection"})

        result = json.loads(response_data.content)
        # print(result)
        if result["status"] == "success":
            if result["face_detection"]["results"]:
                if len(result["face_detection"]["results"]) > 0:
                    if result["face_detection"]["results"][0]["gender"]["decision"] == "male":
                        return "1"
                    elif result["face_detection"]["results"][0]["gender"]["decision"] == "female":
                        return "2"
                    else:
                        return "0"
                else:
                    return "0"
            else:
                return "0"
        elif result["status"] == "failure":
            if result["error"]["errorCode"] == 11:
                api_key_index += 1
            return "-1"

    except Error as error:
        print(error)


if __name__ == "__main__":
    try:
        connection = mysql.connector.connect(host='s1.lgh.li',
                                             database='fs22-sc-prod',
                                             user='fs22-sc',
                                             password='FS22-SC-pw')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS users_pre_processed (
                                   id BIGINT PRIMARY KEY,
                                   user_id BIGINT,
                                   gender VARCHAR(255),
                                   political_affiliation VARCHAR(255),
                                   democrat_following INTEGER,
                                   republican_following INTEGER,
                                   following_base INTEGER,
                                   FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE RESTRICT ON DELETE CASCADE
                                   );''')
            fill_data_user(cursor)
            identify_gender(connection)
            # get_name_from_username(connection)

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
