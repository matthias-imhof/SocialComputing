import csv
import mysql.connector
from mysql.connector import Error



def fillDataUser(cursor):
    sql_baseQuery = """INSERT INTO users_pre_processed_political_affiliation VALUES (%s, %s, %s, %s, %s, %s, %s)"""

    # depending on the size of the table needs to change
    cursor = connection.cursor()
    cursor.execute("select id from users;")
    record = cursor.fetchall()
    for row in record:
        data = (str(row).strip('(),'), str(row).strip('(),'), -1 , -1, -1 , -1, -1)
        cursor.execute(sql_baseQuery, data)
    connection.commit()



def getFollowersAffiliation(cursor):
    cursor = connection.cursor()
    # change to cursor.execute("select user_id,following_id from user_following;")
    cursor.execute("select id,username from users;")
    # depending on the size of the table needs to change
    record = cursor.fetchall()

    politicianDataset = {}

    userFollowing = {}
    userFollowingDemocrats = {}
    userFollowingRepublicans = {}
    polticalAffiliation = {}

    # load politician data from file
    with open('US-Politicians-Twitter-Dataset.csv', 'r') as dataset:
        lines = dataset.readlines()

    for idx,line in enumerate(lines):
        # first row (header row)
        if(idx == 0):
            continue
        # split each line with the , delimeter and have a list to easily select the witter userID [3] and political affiliation[10] also remove last linebreak
        temp = line.split(',')
        politicianDataset[temp[3]] = temp[10][:-1]

    # add followers of a userID to a list. The key being a specific twitter userID
    for row in record:
        if userFollowing.get(row[0]) is not None:
            temp = userFollowing[row[0]]
            temp.append(row[1])
            userFollowing[row[0]] = temp
        else:
            userFollowing[row[0]] = [row[1]]
            userFollowingRepublicans[row[0]] = 0
            userFollowingDemocrats[row[0]] = 0

    # iterate through all users and it's followers
    for userID in userFollowing:
        for following in userFollowing[userID]:
            if following in politicianDataset:
                if (politicianDataset[following] == "Democratic Party"):
                    userFollowingDemocrats[userID] += 1
                elif (politicianDataset[following] == "Republican Party"):
                    userFollowingRepublicans[userID] += 1
                else:
                    continue

    for userID in userFollowing:
        democrats = userFollowingDemocrats.get(userID)
        republicans = userFollowingRepublicans.get(userID)
        followingbase = len(userFollowing.get(userID))
        # TODO: do concrete affiliation calculation and define baseline 0.6

    sql_baseQuery = """UPDATE users_pre_processed_political_affiliation SET political_affiliation = %s, democrat_following = %s, republican_following = %s, following_base = %s WHERE id = %s"""

    # # update records accordingly
    # cursor = connection.cursor()
    # for idx, userID in enumerate(userFollowing):
    #     data = (polticalAffiliation.get(userId), userFollowingDemocrats.get(userID), userFollowingRepublicans.get(userID), len(userFollowing.get(userID)), userID)
    #     cursor.execute(sql_baseQuery, data)
    # connection.commit()


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
            cursor.execute('''CREATE TABLE IF NOT EXISTS users_pre_processed_political_affiliation (
                                   id BIGINT PRIMARY KEY,
                                   user_id BIGINT,
                                   gender VARCHAR(255),
                                   political_affiliation VARCHAR(255),
                                   democrat_following INTEGER,
                                   republican_following INTEGER,
                                   following_base INTEGER,
                                   FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE RESTRICT ON DELETE CASCADE
                                   );''')

            # initially fills table with key, foreign-key of users and -1 values for the rest of the columns.
            #fillDataUser(cursor)

            getFollowersAffiliation(cursor)


    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
