"""Creates a MongoEngine collection from the csv source"""

import sys
import csv
from urlparse import urlparse
from collections import namedtuple

from mongoengine import (
        connect, Document, IntField, StringField, 
        PointField)


class ZipGeo(Document):
    zip_code = IntField(required=True, unique=True)
    lonlat = PointField(required=True)
    location = StringField(required=True)
    state = StringField(required=True)
    timezone = IntField()
    dst = IntField()


ZipRow = namedtuple('ZipRow',
                    ('zip_code', 'location', 'state', 'lat', 'lon', 'tz', 'dst',))



def generate_zip_rows():
    """Generates ZipRow instances from the csv rows"""

    with open('zipcode.csv', 'rb') as zipsource:
        zipdata = csv.reader(zipsource, delimiter=',')
        for row in zipdata:
            try:
                yield ZipRow._make(row)
            except TypeError:
                pass


if __name__ == '__main__':
    MONGO_DEFAULT = 'mongodb://localhost/geodb'
    try:
        firstarg = sys.argv[1]
    except IndexError:
        firstarg = None
        print 'Using default url to connect: {}'.format(MONGO_DEFAULT)

    if firstarg == 'help': 
        print 'Usage: python zipgeo.py <db_url>'
        print 'example: python zipgeo.py mongodb://localhost/geodb'
        raise SystemExit()

    MONGO_URL = urlparse(firstarg if firstarg else MONGO_DEFAULT) 
    MONGO_DB = MONGO_URL.path[1:]
    MONGO_HOST = MONGO_URL.hostname
    MONGO_PORT = MONGO_URL.port
    MONGO_USER = MONGO_URL.username
    MONGO_PASS = MONGO_URL.password

    conn = connect(
              MONGO_DB, host=MONGO_HOST, port=MONGO_PORT,
              username=MONGO_USER, password=MONGO_PASS)
    if MONGO_USER and MONGO_PASS:
            conn[MONGO_DB].authenticate(MONGO_USER, MONGO_PASS)
    for row in generate_zip_rows():
        try:
            ZipGeo(
                zip_code=int(row.zip_code), lonlat=[float(row.lon), float(row.lat)],
                location=row.location, state=row.state, timezone=int(row.tz),
                dst=int(row.dst)).save()
        except ValueError:
            pass
