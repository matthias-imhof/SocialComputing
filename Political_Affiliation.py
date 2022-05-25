import csv
import mysql.connector
from mysql.connector import Error



# fills up values only for users who are not yet in the DB
def fillDataUser(cursor):
    sql_baseQuery = """INSERT INTO users_pre_processed_political_affiliation VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    sql_checkQuery = """select id from users_pre_processed_political_affiliation WHERE id = %s"""

    # depending on the size of the table needs to change
    cursor = connection.cursor()
    cursor.execute("select id from users;")
    record = cursor.fetchall()

    # go through all the rows of the user table
    for row in record:
        # get only the twitteruserid
        userID = str(row).strip('(),')
        # check if the user from the users table is in the users_pre_processed table
        cursor.execute(sql_checkQuery, [userID])
        userIDdb = cursor.fetchall()
        if(len(userIDdb) > 0):
            print('already processed user: ' + str(userID))
            continue
        else:
            data = (str(row).strip('(),'), str(row).strip('(),'), -1 , -1, -1 , -1, -1)
            cursor.execute(sql_baseQuery, data)
    connection.commit()



def getFollowersAffiliation(cursor):
    cursor = connection.cursor()
    # TODO: change to cursor.execute("select user_id,following_id from user_following;")
    cursor.execute("select id,username from users;")
    # depending on the size of the table needs to change
    record = cursor.fetchall()

    # key = twitterID of polictican,    value = Political party (either Democratic Party or Republican Party)
    politicianDataset = {}
    # key = twitterID of user,      value = list of twitterID's of people that the person is following
    userFollowing = {}
    # key = twitterID of user,      value = number of democratic accounts a user follows
    userFollowingDemocrats = {}
    # key = twitterID of user,      value = number of republican accounts a user follows
    userFollowingRepublicans = {}
    # key = twitterID of user,      value = string of the users political affilialiation (-1 = not yet processed, 0 undefined, 1 = democrat, 2 = republican)
    polticalAffiliation = {}

    # load politician data from file into dict
    with open('US-Politicians-Twitter-Dataset.csv', 'r') as dataset:
        lines = dataset.readlines()
    for idx,line in enumerate(lines):
        # first row (header row with description can be dropped)
        if(idx == 0):
            continue
        # split each line with the , delimeter and have a list to easily select the witter userID [3] and political affiliation[10] also remove last linebreak
        temp = line.split(',')
        politicianDataset[temp[3]] = temp[10][:-1]

    # add followers of a userID to a list. The key being a specific twitter userID
    # only process twitter users which are not yet processed (i.e. have -1 in the political affiliation column)
    sql_checkQuery = """select id from users_pre_processed_political_affiliation WHERE id = %s AND political_affiliation > -1"""
    for row in record:
        if userFollowing.get(row[0]) is not None:
            temp = userFollowing[row[0]]
            temp.append(row[1])
            userFollowing[row[0]] = temp
        else:
            # twitter user is already processed and can be skipped will be determined based on the political affilian which is -1 if not proessed and otherwise greater
            cursor.execute(sql_checkQuery, [row[0]])
            userIDdb = cursor.fetchall()
            if (len(userIDdb) > 0):
                print('already processed user: ' + str(row[0]))
                continue
            else:
                userFollowing[row[0]] = [row[1]]
                userFollowingRepublicans[row[0]] = 0
                userFollowingDemocrats[row[0]] = 0

    # iterate through all users and it's followers and find out if following person is a politician from the dataset
    for userID in userFollowing:
        for following in userFollowing[userID]:
            if following in politicianDataset:
                if (politicianDataset[following] == "Democratic Party"):
                    userFollowingDemocrats[userID] += 1
                elif (politicianDataset[following] == "Republican Party"):
                    userFollowingRepublicans[userID] += 1
                else:
                    continue

    sql_updateQuery = """UPDATE users_pre_processed_political_affiliation SET political_affiliation = %s, democrat_following = %s, republican_following = %s, following_base = %s WHERE id = %s"""

    for userID in userFollowing:
        democrats = userFollowingDemocrats.get(userID)
        republicans = userFollowingRepublicans.get(userID)
        # Do concrete affiliation calculation and define baseline 0.6
        # there can't be a devision by 0 error
        if ((democrats + republicans) < 5):
            polticalAffiliation[userID] = 0
        elif(democrats > republicans):
            if ((democrats) / (democrats + republicans) >= 0.6):
                polticalAffiliation[userID] = 1
            else:
                polticalAffiliation[userID] = 0
        elif (republicans > democrats):
            if ((republicans) / (democrats + republicans) >= 0.6):
                polticalAffiliation[userID] = 2
            else:
                polticalAffiliation[userID] = 0
        # if equal followers
        else:
            polticalAffiliation[userID] = 0
    # update records accordingly
    print(polticalAffiliation)
    cursor = connection.cursor()
    # for idx, userID in enumerate(userFollowing):
    #     data = (polticalAffiliation.get(userId), userFollowingDemocrats.get(userID), userFollowingRepublicans.get(userID), len(userFollowing.get(userID)), userID)
    #     cursor.execute(sql_updateQuery, data)
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
                                   gender INTEGER,
                                   political_affiliation INTEGER,
                                   democrat_following INTEGER,
                                   republican_following INTEGER,
                                   following_base INTEGER,
                                   FOREIGN KEY (user_id) REFERENCES users (id) ON UPDATE RESTRICT ON DELETE CASCADE
                                   );''')

            # initially fills table with key, foreign-key of users and -1 values for the rest of the columns.
            fillDataUser(cursor)

            getFollowersAffiliation(cursor)


    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
