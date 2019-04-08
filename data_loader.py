import urllib
import json
from pandas.io.json import json_normalize
from pprint import pprint
import psycopg2
import csv

# tbd
# refactor and put everything into functions/classes
# add try/catch
# rework the gdp inserts

# DB connection
conn = psycopg2.connect("dbname=worldbank user=postgres password=xxxXXXxxx")

def cleanupstr(string):
    new_string = string.replace("'","")
    return new_string

#
# CREATE TABLES
#

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE countryData (
            countryId VARCHAR(6)  PRIMARY KEY,
            countryCode VARCHAR(6) NOT NULL,
            country VARCHAR(128) NOT NULL,
            countryCapital VARCHAR(64) NOT NULL,
            countryRegion VARCHAR(64) NOT NULL      
        )
        """,
        """ CREATE TABLE incomeData (
                incomeLevelId VARCHAR(6) NOT NULL,
                incomeLevel VARCHAR(64) NOT NULL
                )
        """,
        """
        CREATE TABLE lendingData (
                lendingTypeId VARCHAR(6) NOT NULL,
                lendingTypeIsoCode VARCHAR(6) NOT NULL,
                lendingType VARCHAR(64) NOT NULL
        )
        """,
        """
        CREATE TABLE CountryIncomeLending (
                CountryIncomeLendingId INTEGER PRIMARY KEY,
                countryId VARCHAR(6) NOT NULL,
                incomeLevelId VARCHAR(6) NOT NULL,
                lendingTypeId VARCHAR(6) NOT NULL
        )
        """,
        """ 
        CREATE TABLE GEPDataTemp (
                CountryName VARCHAR(40) NOT NULL,
                CountryCode VARCHAR(40) NOT NULL,
                IndicatorName VARCHAR(40) NOT NULL,
                IndicatorCode VARCHAR(40) NOT NULL,
                d1999 VARCHAR(16) NOT NULL,
                d2000 VARCHAR(16) NOT NULL,
                d2001 VARCHAR(16) NOT NULL,
                d2002 VARCHAR(16) NOT NULL,
                d2003 VARCHAR(16) NOT NULL,
                d2004 VARCHAR(16) NOT NULL,
                d2005 VARCHAR(16) NOT NULL,
                d2006 VARCHAR(16) NOT NULL,
                d2007 VARCHAR(16) NOT NULL,
                d2008 VARCHAR(16) NOT NULL,
                d2009 VARCHAR(16) NOT NULL,
                d2010 VARCHAR(16) NOT NULL,
                d2011 VARCHAR(16) NOT NULL,
                d2012 VARCHAR(16) NOT NULL,
                d2013 VARCHAR(16) NOT NULL,
                d2014 VARCHAR(16) NOT NULL,
                d2015 VARCHAR(16) NOT NULL,
                d2016 VARCHAR(16) NOT NULL,
                d2017 VARCHAR(16) NOT NULL,
                d2018 VARCHAR(16) NOT NULL,
                d2019 VARCHAR(16) NOT NULL,
                d2020 VARCHAR(16) NOT NULL,
                d2021 VARCHAR(16) NOT NULL
        )
        """,
        """ 
        CREATE TABLE GEPData (
                countryId VARCHAR(16) NOT NULL,
                indicatorcode VARCHAR(16) NOT NULL,
                year VARCHAR(26) not null,
                gdp VARCHAR(6) null
        )
        """,
        )

    try:
        # read the connection parameters
        #params = config()
        # connect to the PostgreSQL server
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

create_tables()

#
# API CALL AND INSERT WORLDBANK INFO INTO TABLES
#

request = urllib.request.Request('http://api.worldbank.org/v2/country?per_page=1000&format=json')
opener = urllib.request.build_opener()
response = opener.open(request)
elevations = response.read()
data = json.loads(elevations)
CountryIncomeLendingId = 1

cur = conn.cursor()

for datas in data[1]:
    #print(datas)

    #countryData
    countryId = cleanupstr(datas['id'])
    countryCode = cleanupstr(datas['iso2Code'])
    country = cleanupstr(datas['name'])
    countryCapital = cleanupstr(datas['capitalCity'])
    countryRegion = cleanupstr(datas['region']['value'])

    #incomeData
    incomeLevelId = cleanupstr(datas['incomeLevel']['id'])
    incomeLevel = cleanupstr(datas['incomeLevel']['value'])

    #lendingData
    lendingTypeId = cleanupstr(datas['lendingType']['id'])
    lendingTypeIsoCode = cleanupstr(datas['lendingType']['iso2code'])
    lendingType = cleanupstr(datas['lendingType']['value'])

    sqlInsertCountry = "INSERT INTO countryData(countryId,countryCode,country,countryCapital,countryRegion) VALUES('"\
    + countryId + "','"+ countryCode + "','"+ country + "','"+ countryCapital + "','" + countryRegion + "')"

    sqlInsertIncome = "INSERT INTO incomeData(incomeLevelId,incomeLevel) VALUES('" \
    + incomeLevelId + "','" + incomeLevel + "')"

    sqlInsertLending = "INSERT INTO lendingData(lendingTypeId,lendingTypeIsoCode,lendingType) VALUES('" \
    + lendingTypeId + "','" + lendingTypeIsoCode + "','" + lendingType + "')"

    sqlInsertCountryIncomeLending = "INSERT INTO CountryIncomeLending(CountryIncomeLendingId,countryId,incomeLevelId,lendingTypeId) VALUES('" \
    + str(CountryIncomeLendingId) + "','" + countryId + "','" + incomeLevelId + "','" + lendingTypeId + "')"

    CountryIncomeLendingId += 1

    print(sqlInsertCountry)
    print(sqlInsertIncome)
    print(sqlInsertLending)
    print(sqlInsertCountryIncomeLending)

    cur.execute(sqlInsertCountry)
    cur.execute(sqlInsertIncome)
    cur.execute(sqlInsertLending)
    cur.execute(sqlInsertCountryIncomeLending)
    conn.commit()

#
# CSV IMPORT
#

with open('GEPData.csv') as csv_file:

    # create table one by one
    # close communication with the PostgreSQL database server

    csv_reader = csv.reader(csv_file, quotechar='"', delimiter=',')

    for row in csv_reader:
        insertGEPDAta = "Insert Into gepdatatemp(CountryName,CountryCode,IndicatorName,IndicatorCode,d1999,d2000,d2001,d2002,d2003,d2004,d2005,d2006,d2007,d2008,d2009,d2010,d2011,d2012,d2013,d2014,d2015,d2016,d2017,d2018,d2019,d2020,d2021) VALUES("
        for row1 in row:
            insertGEPDAta += "'" + cleanupstr(row1) + "',"
        insertGEPDAta += "')"
        print(insertGEPDAta)
        print(insertGEPDAta.replace(",'',')",")"))
        cur = conn.cursor()

        cur.execute(insertGEPDAta.replace(",'',')",")"))
        cur.close()
        # commit the changes
        conn.commit()

cur = conn.cursor()

# below in necessary to reorganize the code. If I had more time I would refactor the above code to insert the data differently
cur.execute('''insert into GEPData(countryId,indicatorcode,year,gdp)
(select countrycode,indicatorcode,'1999',d1999 from gepdatatemp)
UNION ALL
(select countrycode,indicatorcode,'2000',d2000 from gepdatatemp)
UNION ALL
(select countrycode,indicatorcode,'2001',d2001 from gepdatatemp)
UNION ALL
(select countrycode,indicatorcode,'2002',d2002 from gepdatatemp)
UNION all 
(select countrycode,indicatorcode,'2003',d2003 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2004',d2004 from gepdatatemp)
UNION ALL
(select countrycode,indicatorcode,'2005',d2005 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2006',d2006 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2007',d2007 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2008',d2008 from gepdatatemp)
UNION ALL
(select countrycode,indicatorcode,'2009',d2009 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2010',d2010 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2011',d2011 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2012',d2012 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2013',d2013 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2014',d2014 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2015',d2015 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2016',d2016 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2017',d2017 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2018',d2018 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2019',d2019 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2020',d2020 from gepdatatemp)
UNION all
(select countrycode,indicatorcode,'2021',d2021 from gepdatatemp)''')
cur.close()
# commit the changes
conn.commit()

