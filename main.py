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
#print(count)
dataCollection = db['pm2p5']   # get collection of PM2.5 data
aliasCollection = db['sensorAliases']  # get the aliases and sensor IDs

ID_LIST = []
new_values = []


def get_data_doc(year, month, day, hour, minute, sensorID, value):
    """
        Creates and returns a pm2.5 value document for the database.
        Date and time must be UTC.
    """
    return {
        'DATETIME': datetime.datetime(year, month, day, hour, minute),
        'ID': sensorID,
        'Value': value
    }


def get_latest_value(sensor):
    """
        Returns the latest document in the DB of the sensorID.
        DB is automatically indexed with ID (ascending) and DATETIME (descending), so the latest reading (in time) is the first one.
    """
    latest = pm2p5.find_one({'ID': sensor['Sensor ID']}, {'_id': 0})
    return latest


def add_data(arrayOfDataDocs):
    """
        Inserts an array of docs into the pm2p5 collection.
    """
    pm2p5.insert_many(arrayOfDataDocs)


def get_values(sensorAlias, startYear, startMonth, startDay, endYear, endMonth, endDay):
    """
        Gets a sensor's data between two dates.
        The input sensorAlias is the alias of the sensors to get data from
    """
    # get the actual sensor IDs of sensors with the alias
    sensorIDs = aliasCollection.find({'Alias': sensorAlias})

    valuesInDateRange = []
    # iterate through each sensorID and append the data into one list
    for sensor in sensorIDs:
        sensorData = list(pm2p5.find({'ID': sensor['Sensor ID'],
                                            'DATETIME': {'$gte': datetime.datetime(startYear, startMonth, startDay),
                                            '$lt': datetime.datetime(endYear, endMonth, endDay)}},
                                            {'_id': 0}))
        valuesInDateRange = valuesInDateRange + sensorData
    
    valuesInDateRange = list(filter(None, valuesInDateRange))

    return valuesInDateRange


# build id list from database
def build_id_list(new_id):
    #print('LENGTH OF ID LIST: ', len(new_id))
    for x in new_id:
        if x not in ID_LIST:
            ID_LIST.append(x)
        else:
            pass
    #print("LIST BUILT")


# get em the top new values from distinct id list
def get_values_latest(id_list_tmp):
    new_values.clear()
    for y in id_list_tmp:
        latestVal = get_latest_value(y)
        if latestVal is not None:
            new_values.append({'Alias': y['Alias'], 'Value': latestVal['Value'], 'DATETIME': latestVal['DATETIME']})
    
    #pprint(new_values)


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
    #run(host='ec2-54-241-231-119.us-west-1.compute.amazonaws.com', port=3000)
    currentSensors = list(aliasCollection.find({'Current': True}, {'Lat': 0, 'Long': 0, '_id': 0, 'Current': 0}))

    build_id_list(currentSensors)
    #pprint(ID_LIST)

    get_values_latest(ID_LIST)
    pprint(new_values)
    #pprint(get_values('paideia1', 2021, 1, 1, 2021, 1, 2))

# it makes you do this
if __name__ == "__main__":
    main()
