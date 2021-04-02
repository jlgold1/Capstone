# mssql_db_interface.py
# For the Python Bottle server to add and update data on the MS SQL DB.
# must import pyodbc

import pyodbc   # python driver for SQL server

SERVER = 'localhost\SQLEXPRESS' # server location
DATABASE = 'SensorData'
sensorDetailsTable = '.dbo.SensorDetails'
pm2p5CalTable = '.dbo.PM2p5Calibrated'
connection = pyodbc.connect('Driver={SQL Server};SERVER=' + SERVER + 
                            ';DATABASE=' + DATABASE + 
                            ';Trusted_Connection=yes;')

cursor = connection.cursor()
# test connection
#cursor.execute('SELECT TOP 1 * FROM SensorData.dbo.PM2p5Calibrated')
#for row in cursor:
#    print(row)

def insert_new_data(sensor_id, datetime, value):
    '''
        Insert a new tuple into the PM2p5Calibrated table.
        sensorID is an 8-character string
        dateTime is the exact dat/time string from the Clarity API
        value is the decimal PM2.5 reading
    '''
    cursor.execute('''INSERT INTO SensorData.dbo.PM2p5Calibrated (DateTime, SensorID, Value) VALUES (?, ?, ?)''', 
                    datetime, sensor_id, value)
    cursor.commit()

def insert_sensor(sensor_id, alias, is_current, latitude, longitude):
    '''
        Adds a new sensor to sensorDetails.
        Might not be used that often.
        sensor_id is the 8-character long sensor ID
        is_current is '1' (true) or '0' (false)
        latitude and longitude are both of type float
    '''
    # if other sensors share this alias and the given is_current is '1', change the IsCurrent values to '0'
    if(is_current == '1'):
        cursor.execute('''UPDATE SensorData.dbo.SensorDetails SET IsCurrent = '0' WHERE Alias = (?)''', alias)
    
    cursor.execute('''INSERT INTO SensorData.dbo.SensorDetails (SensorID, Alias, IsCurrent, Lat, Long) VALUES (?, ?, ?, ?, ?)''',
                    sensor_id, alias, is_current, latitude, longitude)
    cursor.commit()

# test
#insert_sensor('sensorA', 'alias', '1', 1, 0)
#insert_sensor('sensorB', 'alias', '1', 1, 0)
#insert_new_data('sensor1', '2021-04-01 12:00:00', 1.2345)
#insert_new_data('sensor2', '2021-04-01 12:00:00', 1.2345)
#insert_new_data('sensor3', '2021-04-01 12PM', 1.2345)