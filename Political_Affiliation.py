import time

import mysql.connector
from mysql.connector import Error
from tqdm import tqdm


# fills up values only for users wh     o are not yet in the DB
def fillDataUser(cursor):
    sql_baseQuery = """INSERT INTO users_pre_processed_v2 VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    #sql_checkQuery = """select id from users_pre_processed_v2 WHERE id = %s"""

    # depending on the size of the table needs to change
    cursor = connection.cursor()
    cursor.execute("SELECT id FROM users WHERE id NOT IN (SELECT id from users_pre_processed_v2);")
    # where
    record = cursor.fetchall()

    # NEXT IDEA -> Fetch always 100k and fill table with values the below

    if(len(record) >= 30000):
        counter = [i for i in range(0, len(record), 30000)]
        counter.append(len(record))
    else:
        counter = []
        counter.append(0)
        counter.append(len(record))


    for pos, element in tqdm(enumerate(counter), desc='filling missing values'):
        if pos+1 < len(counter):
            data = [(str(record[row]).strip('(),'), str(record[row]).strip('(),'), -1, -1, -1, -1, -1) for row in range(element, counter[pos + 1])]
            cursor.executemany(sql_baseQuery, data)

    connection.commit()


def getFollowersAffiliation(cursor):
    cursor = connection.cursor()
    # TODO: change to cursor.execute("select user_id,following_id from user_following;")
    cursor.execute("select user_id,following_id from user_following inner join users on following_id = users.id inner join politicians on users.username = politicians.username;")
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
    for idx, line in tqdm(enumerate(lines), desc='Load Politicians twitter ID into dict'):
        # first row (header row with description can be dropped)
        if (idx == 0):
            continue
        # split each line with the , delimeter and have a list to easily select the witter userID [3] and political affiliation[10] also remove last linebreak
        temp = line.split(',')
        politicianDataset[temp[3]] = temp[10][:-1]

    # add followers of a userID to a list. The key being a specific twitter userID
    # only process twitter users which are not yet processed (i.e. have -1 in the political affiliation column)
    sql_checkQuery = """select id from users_pre_processed_v2 WHERE id = %s AND political_affiliation != -1"""


    for row in tqdm(record, desc='Find following twitter user'):

        if userFollowing.get(row[0]) is not None:
            cursor.execute(sql_checkQuery, [row[0]])
            userIDdb = cursor.fetchall()
            if len(userIDdb) > 0:
                #print('already processed user: ' + str(row[0]))
                continue
            else:
                #print('NOT YET PROCESSED USER: ' + str(row[0]) + " with following: " + str(row[1]))
                temp = userFollowing[row[0]]
                temp.append(row[1])
                userFollowing[row[0]] = temp
        else:
            # twitter user is already processed and can be skipped will be determined based on the political affilian which is -1 if not proessed and otherwise greater
            cursor.execute(sql_checkQuery, [row[0]])
            userIDdb = cursor.fetchall()
            if (len(userIDdb) > 0):
                #print('already processed user: ' + str(row[0]))
                continue
            else:
                #print('NOT YET PROCESSED USER: ' + str(row[0]) + " with following: " + str(row[1]))
                userFollowing[row[0]] = [row[1]]
                userFollowingRepublicans[row[0]] = 0
                userFollowingDemocrats[row[0]] = 0

    # iterate through all users and it's followers and find out if following person is a politician from the dataset

    for userID in tqdm(userFollowing, desc='Counting individual party'):
        for followingID in userFollowing[userID]:
            if str(followingID) in politicianDataset:
                if "dem" in politicianDataset[str(followingID)].lower():
                    userFollowingDemocrats[userID] += 1
                elif "rep" in politicianDataset[str(followingID)].lower():
                    userFollowingRepublicans[userID] += 1
                else:
                    continue
            else:
                continue

    sql_updateQuery = """UPDATE users_pre_processed_v2 SET political_affiliation = %s, democrat_following = %s, republican_following = %s, following_base = %s WHERE id = %s"""

    for userID in tqdm(userFollowing, desc='Political definition function'):
        democrats = userFollowingDemocrats.get(userID)
        republicans = userFollowingRepublicans.get(userID)
        # Do concrete affiliation calculation and define baseline 0.6
        # there can't be a devision by 0 error
        if (democrats + republicans) < 5:
            polticalAffiliation[userID] = 0
            continue
        elif democrats > republicans:
            if (democrats) / (democrats + republicans) >= 0.6:
                polticalAffiliation[userID] = 1
            else:
                polticalAffiliation[userID] = 0
            continue
        elif republicans > democrats:
            if republicans / (democrats + republicans) >= 0.6:
                polticalAffiliation[userID] = 2
            else:
                polticalAffiliation[userID] = 0
            continue
        # if equal followers
        else:
            polticalAffiliation[userID] = 0
    # update records accordingly
    #print(userFollowing)
    #print(userFollowingDemocrats)
    #print(userFollowingRepublicans)
    #print(polticalAffiliation)
    cursor = connection.cursor()
    # for idx, userID in enumerate(userFollowing):
    #     data = (polticalAffiliation.get(userID), userFollowingDemocrats.get(userID), userFollowingRepublicans.get(userID), len(userFollowing.get(userID)), userID)
    #     cursor.execute(sql_updateQuery, data)

    data = [(polticalAffiliation.get(userID), userFollowingDemocrats.get(userID), userFollowingRepublicans.get(userID), len(userFollowing.get(userID)), userID) for userID in userFollowing]
    cursor.executemany(sql_updateQuery, data)

    if (len(userFollowing) >= 30000):
        counter = [i for i in range(0, len(userFollowing), 30000)]
        counter.append(len(userFollowing))
    else:
        counter = []
        counter.append(0)
        counter.append(len(userFollowing))


    userIDs = list(userFollowing)

    for pos, element in tqdm(enumerate(counter), desc='filling missing values'):
        if (pos + 1 < len(counter)):
            data = [(polticalAffiliation.get(userIDs[userID]), userFollowingDemocrats.get(userIDs[userID]), userFollowingRepublicans.get(userIDs[userID]), len(userFollowing.get(userIDs[userID])), userIDs[userID]) for userID in
                    range(element, counter[pos + 1])]
            cursor.executemany(sql_updateQuery, data)


    connection.commit()


if __name__ == "__main__":
    try:
        # fs22-sc-test
        # TestDB
        connection = mysql.connector.connect(host='s1.lgh.li',
                                             database='fs22-sc-prod',
                                             user='fs22-sc',
                                             password='FS22-SC-pw')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS users_pre_processed_v2 (
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
