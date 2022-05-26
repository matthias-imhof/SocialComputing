import csv
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import Error
import pandas as pd
import json

import requests
import os

# Pandas DB connection to MySQL


# curl -X POST 'https://www.picpurify.com/analyse/1.1' -F 'API_KEY=XXX' -F 'task=porn_moderation,drug_moderation,gore_moderation' -F 'origin_id=xxxxxxxxx' -F 'reference_id=yyyyyyyy' -F 'file_image=@/path/to/local/file.jpg'


picpurify_api_keys = ['o9F9c4RNiYCrZNIZkPa6PIeL7YdihXCT', '4hhAWQnp6qiA9jDwr5wlgw3XZb3y9DJK',
                      '9Vgra9ab0ic2EEkGIhuHlSiZ8XWr9UeL']

picpurify_url = 'https://www.picpurify.com/analyse/1.1'


def fill_data_user(cursor):
    sql_baseQuery = """INSERT INTO users_pre_processed_gender_test VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    # depending on the size of the table needs to change
    cursor = connection.cursor()
    cursor.execute("select id from users;")
    record = cursor.fetchall()
    for row in record:
        data = (str(row).strip('(),'), str(row).strip('(),'), -1, -1, -1, -1, -1)
        cursor.execute(sql_baseQuery, data)
    connection.commit()


def sendQuery(cursor, query_string, data):
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM users;")
    record = cursor.fetchall()
    for row in record:
        cursor.execute(query_string, data)
    connection.commit()


def read_name_files(file):
    return pd.read_csv(file, dtype=str, na_values=["?", "NaN", "nan", "NAN"], na_filter=True)


def get_name_from_username(db_connection):
    users = pd.read_sql_query("SELECT id, username, name FROM users;", db_connection)
    user_preprocessed_names = []
    # print(user_preprocessed_names)
    # print(users)
    for index, row in users.iterrows():
        row_name = row["name"].split(" ")[0]
        print(row_name)


def get_classified_names():
    names_data_us = read_name_files("name_gender_dataset.csv").drop_duplicates("Name")
    names_data_global = read_name_files("wgnd_ctry.csv").dropna(subset=["gender"])

    name_gender_df_us = pd.DataFrame(
        {"Name": names_data_us["Name"].str.lower(), "Gender": names_data_us["Gender"]})
    name_gender_df_global = pd.DataFrame(
        {"Name": names_data_global["name"].str.lower().str.replace(" ", ""),
         "Gender": names_data_global["gender"]})

    all_names_gender_df = pd.concat([name_gender_df_global, name_gender_df_us]).drop_duplicates()
    return name_gender_df_us


def get_users(db_connection):
    return pd.read_sql_query("SELECT id, username, name ,profile_picture_url FROM users;", db_connection)


def identify_gender(db_connection):
    # TODO: maybe expand list
    not_names = ["the", "and", "stars", "democrat", "republican", "rep", "dem"]

    users_df = get_users(db_connection)
    gendered_names = dict(zip(get_classified_names()['Name'], get_classified_names()["Gender"]))
    preprocessed_users = pd.read_sql_query("SELECT user_id, gender FROM users_pre_processed_gender_test", db_connection)
    # print(preprocessed_users)

    indexes = []
    result = {}

    api_calls_count = 0
    gender_code = -1

    for index, user in users_df.iterrows():
        name = user["name"].split(" ")[0].lower()
        if name in not_names:
            continue
        if len(name) < 3:
            continue
        if name in gendered_names:

            print(users_df.iloc[index]["id"])
            result[user["username"]] = gendered_names[name]
            indexes.append(users_df.iloc[index]["id"])
            if result[user["username"]] == "M":
                gender_code = "1"
            else:
                gender_code = "2"
            query_string = "UPDATE users_pre_processed_gender_test SET gender=%s WHERE id=%s"
            data = (gender_code, int(users_df.iloc[index]["id"]))
            sendQuery(cursor, query_string, data)
        elif preprocessed_users.iloc[index]["gender"] == "-1":
            url = user["profile_picture_url"]
            query_string = "UPDATE users_pre_processed_gender_test SET gender=%s WHERE id=%s"
            data = (classify_gender_by_pic(url), int(users_df.iloc[index]["id"]))

            sendQuery(cursor, query_string, data)
            api_calls_count += 1
        else:
            print(name, "Gender unidentifiable")

    df = pd.DataFrame({"id": indexes, "gender": result.values()})
    print(api_calls_count)

    return result


def classify_gender_by_pic(picture_url):
    try:
        response_data = requests.post(picpurify_url, data={
            "url_image": picture_url,
            "API_KEY": picpurify_api_keys[0], "task": "face_gender_detection"})

        result = json.loads(response_data.content)
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
        else: return "0"
    except Error as error:
        print(error)


if __name__ == "__main__":
    try:
        connection = mysql.connector.connect(host='s1.lgh.li',
                                             database='fs22-sc',
                                             user='fs22-sc',
                                             password='FS22-SC-pw')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS users_pre_processed_gender_test (
                                   id BIGINT PRIMARY KEY,
                                   user_id BIGINT,
                                   gender VARCHAR(255),
                                   political_affiliation VARCHAR(255),
                                   democrat_following INTEGER,
                                   republican_following INTEGER,
                                   following_base INTEGER,
                                   FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE RESTRICT ON DELETE CASCADE
                                   );''')
            print(identify_gender(connection))
            # get_name_from_username(connection)

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
