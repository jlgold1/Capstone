# main.py
# Interacts with the MongoDB cluster, Clarity API, and ArcGIS dashboard.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# Need to install PyMongo first by doing "python -m pip install 'pymongo[srv]'"
from bottle import route, run, template, get, post, request # bottle server
from pymongo import MongoClient # MongoDB
import datetime

from pprint import pprint  # pretty print for debugging

client = MongoClient('mongodb+srv://bottle_server:#poOp480@pm2p5.bap2s.mongodb.net/data?retryWrites=true&w=majority')
db = client['data']  # access the 'data' db

# show server status
# serverStatusResult = db.command("serverStatus")
# pprint(serverStatusResult)

# access collections
pm2p5 = db['pm2p5']  # collection of PM2.5 data
count = pm2p5.count_documents({})

ID_LIST = []
new_values = []


# print(count)

def printCursor(cursor):
    for c in cursor:
        pprint(c)


def getDoc(year, month, day, hour, minute, sensorID, value):
    """
        Creates and returns a document for the database.
        Date and time must be UTC.
    """
    return {
        'DATETIME': datetime.datetime(year, month, day, hour, minute),
        'ID': sensorID,
        'Value': value
    }


def getLatestValue(sensorID):
    """
        Returns the latest document in the DB of the sensorID.
        DB is automatically indexed with ID (ascending) and DATETIME (descending), so the latest reading (in time) is the first one.
    """
    latest = pm2p5.find_one({'ID': sensorID}, {'_id': 0})
    # pprint(latest)
    return latest


def addData(arrayOfDocs):
    """
        Inserts an array of docs into the pm2p5 collection.
    """
    pm2p5.insert_many(arrayOfDocs)


def getValues(sensorID, startYear, startMonth, startDay, endYear, endMonth, endDay):
    """
        Gets a sensors data between two dates.
    """
    values = pm2p5.find({'ID': sensorID,
                         'DATETIME': {'$gte': datetime.datetime(startYear, startMonth, startDay),
                                      '$lt': datetime.datetime(endYear, endMonth, endDay)}},
                        {'_id': 0})
    return values


# build id list from database
def build_id_list(new_id):
    print('LENGTH OF ID LIST: ', len(new_id))
    for x in new_id:
        if x not in ID_LIST:
            ID_LIST.append(x)
        else:
            pass
    print("LIST BUILT")


# get em the top new values from distinct id list
def get_values_latest(id_list_tmp):
    new_values.clear()
    for y in id_list_tmp:
        new_values.append(getLatestValue(y))
    pprint(new_values)



@get('/login')  # or @route('/login')
def login():
    return '''
        <form action="/login" method="post">
            Username: <input name="username" type="text" />
            Password: <input name="password" type="password" />
            <input value="Login" type="submit" />
        </form>
    '''


def check_login(username, password):
    return True


@post('/login')  # or @route('/login', method='POST')
def do_login():
    username = request.forms.get('username')
    password = request.forms.get('password')
    if check_login(username, password):
        return "<p>Your login information was correct.</p>"
    else:
        return "<p>Login failed.</p>"


# MAIN
def main():
    run(host='ec2-54-241-231-119.us-west-1.compute.amazonaws.com', port=3000)
    collection = db['pm2p5']
    ID_LIST_TMP = pm2p5.find().distinct('ID')
    build_id_list(ID_LIST_TMP)
    get_values_latest(ID_LIST)


# it makes you do this
if __name__ == "__main__":
    main()
