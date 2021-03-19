import psycopg2
import ipinfo
import pprint
import config

def createTable():
    execute("""
    CREATE TABLE IF NOT EXISTS ip_data (
        id SERIAL PRIMARY KEY,
        city VARCHAR(255),
        country VARCHAR(255),
        country_name VARCHAR(255),
        hostname VARCHAR(255),
        ip VARCHAR(255),
        port INTEGER NOT NULL,
        latitude FLOAT ,
        loc VARCHAR(255),
        longitude FLOAT,
        organisation VARCHAR(255),
        postal VARCHAR(10),
        region VARCHAR(255),
        timezone VARCHAR(255)
    );""")


def persist(socket):
    ip, port = socket.getpeername()
    handler = ipinfo.getHandler(config.ACCESS_TOKEN)
    details = handler.getDetails(ip)
    city = details.city
    country = details.country
    country_name = details.country_name
    hostname = details.hostname
    latitude = details.latitude
    loc = details.loc
    longitude = details.longitude
    organisation = details.org
    postal = details.postal
    region = details.region
    timezone = details.timezone
    pprint.pprint(details.all)
    sql = """INSERT INTO vendors(vendor_name)
             VALUES(%s) RETURNING vendor_id;"""
    query = """
    INSERT INTO ip_data(city, country, country_name, hostname, ip, port, latitude, loc, longitude, organisation, postal, region, timezone)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    print(query, city, country, country_name, hostname, ip, port, latitude, loc, longitude, organisation, postal, region, timezone)
    executeWithParams(query, city, country, country_name, hostname, ip, port, latitude, loc, longitude, organisation, postal, region, timezone)


def executeWithParams(query, city, country, country_name, hostname, ip, port, latitude, loc, longitude, organisation, postal, region, timezone):
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="ssh-tarpit",
            user="postgres",
            password="postgres")

        cursor = connection.cursor()
        cursor.execute(query, (city, country, country_name, hostname, ip, port, latitude, loc, longitude, organisation, postal, region, timezone))
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


def execute(query):
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="ssh-tarpit",
            user="postgres",
            password="postgres")

        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
