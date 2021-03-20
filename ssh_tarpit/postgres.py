import psycopg2
import ipinfo
import pprint
import config
import pygeohash


def create_table():
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
        longitude FLOAT,
        loc VARCHAR(255),
        geohash VARCHAR(255),
        organisation VARCHAR(255),
        postal VARCHAR(10),
        region VARCHAR(255),
        timezone VARCHAR(255),
        ts TIMESTAMP WITH TIME ZONE
        );""")


def persist(socket):
    ip, port = socket.getpeername()
    handler = ipinfo.getHandler(config.ACCESS_TOKEN)
    details = handler.getDetails(ip).all
    pprint.pprint(details)
    city = details.get("city")
    country = details.get("country")
    country_name = details.get("country_name")
    hostname = details.get("hostname")
    latitude = float(details.get('latitude', 0))
    longitude = float(details.get('longitude', 0))
    loc = details.get("loc")
    organisation = details.get("org")
    postal = details.get("postal")
    region = details.get("region")
    timezone = details.get("timezone")
    geohash = pygeohash.encode(latitude, longitude)
    query = """
    INSERT INTO ip_data(city, country, country_name, hostname, ip, port, latitude, longitude, loc, geohash , organisation, postal, region, timezone, ts)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP);"""
    execute(query, (city, country, country_name, hostname, ip, port, latitude, longitude, loc, geohash, organisation, postal, region, timezone))


def execute(query, args=()):
    try:
        connection = psycopg2.connect(
            host=config.HOST,
            database=config.DATABASE,
            user=config.USER,
            password=config.PASSWORD)

        cursor = connection.cursor()
        cursor.execute(query, args)
        connection.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
