import base64
import sqlalchemy
import os
import string
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, TIMESTAMP, Numeric, Float, VARCHAR
from sqlalchemy.dialects.postgresql import insert, UUID
from datetime import datetime, timezone
from geoalchemy2 import Geometry, Geography
from geoalchemy2.functions import GenericFunction
from decimal import Decimal

def send_to_postgre(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    
    db_user = os.environ.get("DB_USER")
    db_pass = os.environ.get("DB_PASS")
    db_name = os.environ.get("DB_NAME")
    cloud_sql_connection_name = os.environ.get("CLOUD_SQL_CONNECTION_NAME")

    db = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL(
            drivername="postgres+pg8000",
            username=db_user,
            password=db_pass,
            database=db_name,
            query = { "unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(
                cloud_sql_connection_name) # i.e "<PROJECT-NAME>:<INSTANCE-REGION>:<INSTANCE-NAME>"
            }
        ),
    )

    metadata = MetaData()
    # Create a representation of the table in charge of storing the last records of each RaspPi
    pi_noises = Table('pi_noises', metadata,
        Column('rasp_uuid', UUID, primary_key=True),
        Column('rasp_name', VARCHAR, nullable=False),
        Column('location', Geography(geometry_type='POINT', srid=4326), nullable=False),
        Column('record_date', TIMESTAMP, nullable=False),
        Column('decibels', Float, nullable=False) 
    )

    # Create a representation of the table in charge of storing all the datas

    pi_noises_persistent = Table('pi_noises_persistent', metadata,
        Column('record_id', Integer, primary_key=True),
        Column('rasp_uuid', UUID),
        Column('rasp_name', VARCHAR, nullable=False),
        Column('location', Geography(geometry_type='POINT', srid=4326), nullable=False),
        Column('record_date', TIMESTAMP, nullable=False),
        Column('decibels', Float, nullable=False) 

    # Get information from pubsub message
    data = pubsub_message.split(',')
    latitude = data[0]
    longitude = data[1]
    decibels = Decimal(data[2])
    device_name = data[3]
    device_uuid = data[4]
    date = datetime.utcnow().replace(tzinfo=timezone.utc)
    location = "POINT({} {})".format(latitude, longitude)

    # Insert data in pinoises table or update if the device has already send data
    ins = insert(pi_noises).values(rasp_uuid=device_uuid, rasp_name=device_name, location=location, record_date=date, decibels=decibels)
    ins = ins.on_conflict_do_update(index_elements=['rasp_uuid'], set_=dict(rasp_name=device_name, location=location, record_date=date, decibels=decibels))

    # Insert data in pi_noises_persistent
    ins2 = pi_noises_persistent.insert().values(rasp_uuid=device_uuid, rasp_name=device_name, location=location, record_date=date, decibels=decibels)
    try:
        with db.connect() as conn:
            result = conn.execute(ins)
            result2 = conn.execute(ins2)
    except Exception as e:
        print("Error: {}".format(e))