import mysql.connector
from mysql.connector import Error


def getFollowersAffiliation(cursor):
    cursor = connection.cursor()
    cursor.execute("select * from tweets;")
    record = cursor.fetchall()
    for row in record:
        print(row)



try:
    connection = mysql.connector.connect(host='s1.lgh.li',
                                         database='fs22-sc',
                                         user='fs22-sc',
                                         password='FS22-SC-pw')
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        getFollowersAffiliation(cursor)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")