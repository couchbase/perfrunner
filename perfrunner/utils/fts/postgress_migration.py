# this is migration script for partial export of musicbrainz Postgress DB to couchbase
# the database dump and setup instructions:
#       https://musicbrainz.org/doc/MusicBrainz_Database
#       https://github.com/metabrainz/musicbrainz-server/blob/master/INSTALL.md

# the database is already installed on 172.23.99.210 in root/SEED
# systemctl start postgresql.service

from threading import Thread

import psycopg2
from couchbase.bucket import Bucket
from couchbase.exceptions import NotFoundError


def load_as_separate_docs(bucketname, count, limit):
    bucket_name = bucketname

    print("Connecting to postgres")
    conn = psycopg2.connect("dbname='musicbrainz_db' user='musicbrainz' host='localhost' "
                            "password='musicbrainz'")
    curr = conn.cursor()

    print("Fetching all tracks")
    curr.execute("select id, gid, artist_credit, name, length, comment, edits_pending, "
                 "last_updated from recording offset {} limit {}".format(count, limit))
    rows = curr.fetchall()

    print("Connecting to Couchbase")
    cb = Bucket("couchbase://{}/{}?operation_timeout=10".format(cb_url, bucket_name), password="")

    print("Loading data")
    key_counter = count
    for row in rows:
        document = {}
        subdocument = {}

        document["id"] = row[0]
        document["gid"] = row[1]
        document["artist"] = None
        document["name"] = row[3]
        document["length"] = row[4]
        document["comment"] = row[5]
        document["edits_pending"] = not row[6] == 0
        document["last_updated"] = str(row[7])

        id = row[2]
        query = "select artist_credit_name.artist from artist_credit_name, artist_credit" \
                " where artist_credit_name.artist_credit = artist_credit.id " \
                "and artist_credit.id = {} limit 1".format(id)

        curr.execute(query)
        credit = curr.fetchall()[0]

        query = (
            "select artist.name, artist.sort_name, artist.begin_date_year, "
            "artist.begin_date_month, artist.begin_date_day, artist.end_date_year, "
            "artist.end_date_month, artist.end_date_day, artist.comment, artist.edits_pending, "
            "artist.last_updated, artist.id "
            "from artist where artist.id = {} limit 1"
        ).format(credit[0])

        curr.execute(query)
        artist = curr.fetchall()[0]
        words = artist.split(' ')
        if len(words) > 10:
            words = words[:9]
            artist_key = "{}:".format(artist[11])
            for word in words:
                artist_key = "{} {}".format(artist_key, word)
        else:
            artist_key = "{}:{}".format(artist[11], artist[0])

        try:
            subdocument = cb.get(artist_key).value
        except NotFoundError:
            subdocument = {}
            subdocument["name"] = artist[0]
            subdocument["sort_name"] = artist[1]
            subdocument["begin_date_year"] = artist[2]
            subdocument["begin_date_month"] = artist[3]
            subdocument["begin_date_day"] = artist[4]
            subdocument["end_date_year"] = artist[5]
            subdocument["end_date_month"] = artist[6]
            subdocument["end_date_day"] = artist[7]
            subdocument["comment"] = artist[8]
            subdocument["edits_pending"] = not artist[9] == 0
            subdocument["last_updated"] = str(artist[10])
            subdocument["id"] = artist[11]
            subdocument["track_id"] = list()

        key = hex(key_counter)[2:]

        document["artist"] = artist_key
        subdocument["track_id"].append(key)

        try:
            cb.upsert(key, document)
            cb.upsert(artist_key, subdocument)
            key_counter += 1

        except Exception:
            print(document)
            print(subdocument)


cb_url = "172.23.99.211"

step = 500000
doccounter = 0
while step < 20000000:
    new_thread = Thread(target=load_as_separate_docs, args=("bucket-1", doccounter, step,))
    new_thread.start()
    doccounter += step
