import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from common import DT_FORMAT
from fields import OFFER_FIELDS


conn = psycopg2.connect("host=postgres dbname=flats user=admin password=admin")
conn.set_session(autocommit=True)
cur = conn.cursor(cursor_factory=RealDictCursor)

offer_field_names = ",".join(OFFER_FIELDS.keys())

def get_offers():
    """ Return all offers in the database as JSON """
    cur.execute(f"SELECT {offer_field_names} FROM offers")
    return cur.fetchall()

def get_offer(offer_id):
    """ Get offer by morizon ID """
    cur.execute(f"SELECT {offer_field_names} FROM offers WHERE offer_id__offer = '{offer_id}'")
    return cur.fetchall()

def post_offer(args):
    """ Add offer entry to the database, returned passed offer """
    args_to_post = {}
    for k, v in args.items():
        if v is None:
            continue
        if isinstance(v, str) or isinstance(v, datetime.date):
            v = "'" + str(v) + "'"
        args_to_post[k] = str(v)
    post_fields_names = ",".join(args_to_post.keys())
    post_fields_values= ",".join(args_to_post.values())
    insert_query = f"INSERT INTO offers ({post_fields_names}) VALUES({post_fields_values})"

    raise Exception(insert_query)
    cur.execute(insert_query)
    cur.execute(f"SELECT {offer_field_names} FROM offers WHERE offer_id__offer = '{args['offer_id__offer']}'")
    return cur.fetchall()
