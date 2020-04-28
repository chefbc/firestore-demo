
"""Firestore 'POC'
Usage:
  app.py [--credential_path=<credential_path>] (enter_store|exit_store) <storenum> [--loop=<loop>]
  app.py [--credential_path=<credential_path>] store_details <storenum>
  app.py [--credential_path=<credential_path>] store_counts
  app.py [--credential_path=<credential_path>] reset
  app.py [--credential_path=<credential_path>] populate
  app.py [--credential_path=<credential_path>] create
  app.py (-h | --help)
  app.py --version

Options:
  -h --help                                     Show this screen.
  --version                                     Show version.
  --credential_path=<credential_path>           cloud credentials (full path) [default: demo.json]
  <number>                                      Number of stores to create
  <storenum>                                    Store Number
  --loop=<loop>                                 Loop value [default: 1].
"""

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1 import Increment
from datetime import datetime
from docopt import docopt

db = None

def create_stores(numof_stores):
    store_ref = db.collection('stores')
    for i in range(numof_stores):
        store_id = str(i)
        data = {
            u'name': u'store {0}'.format(store_id),
            u'count': 0,
            u'timestamp': firestore.SERVER_TIMESTAMP
        }
        store_ref.document(store_id).set(data, merge=True)

def update_store(store_num, store_increment_val):
    print (f"Updating store {store_num} by {store_increment_val}")
    batch = db.batch()
    detail_ref = db.collection(u'details').document()
    store_ref = db.collection(u'stores').document(store_num)

    detail_data = {
        u'store_num': store_num,
        u'timestamp': firestore.SERVER_TIMESTAMP,
        u'count': store_ref.get().to_dict().get('count') + store_increment_val
    }

    store_data = {
        u'count': Increment(store_increment_val),
        u'timestamp': firestore.SERVER_TIMESTAMP
    }

    batch.set(detail_ref, detail_data)
    batch.set(store_ref, store_data, merge=True)
    batch.commit()

def update_collection(coll_ref, batch_size , update_dict):
    docs = coll_ref.limit(batch_size).stream()
    updated = 0

    for doc in docs:
        print(u'Update doc {} => {}'.format(doc.id, doc.to_dict()))
        print(u'with => {}'.format(update_dict))
        doc.reference.update(update_dict)
        updated = updated + 1

    if updated >= batch_size:
        return update_collection(coll_ref, batch_size, update_dict)

def store_counts():
    docs = db.collection(u'stores').stream()

    for doc in docs:
        details_dict = doc.to_dict()
        print(u'{} => {}  {}'.format(doc.id, 
            details_dict.get('timestamp').strftime("%m/%d/%Y, %H:%M:%S"), 
            details_dict.get('count')))

def store_details(store_num):
    details_ref = db.collection(u'details').where(u'store_num', u'==', store_num)

    docs = details_ref.order_by(u'timestamp').stream()
    
    for doc in docs:
        details_dict = doc.to_dict()
        print(u'{} => {}  {}'.format(details_dict.get('store_num'), 
            details_dict.get('timestamp').strftime("%m/%d/%Y, %H:%M:%S"), 
            details_dict.get('count')))

def list_collection(coll_ref, batch_size):
    docs = coll_ref.limit(batch_size).stream()

    for doc in docs:
        print(u'List doc {} => {}'.format(doc.id, doc.to_dict()))
        
def delete_collection(coll_ref, batch_size):
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        print(u'Deleting doc {} => {}'.format(doc.id, doc.to_dict()))
        doc.reference.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)

if __name__ == "__main__":
    # load and parse configs
    arguments = docopt(__doc__, version='app 0.2')
    
    #print(arguments)

    json_cred = arguments.get('--credential_path')

    cred = credentials.Certificate(json_cred)
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    if arguments.get('enter_store'):
        for i in range(int(arguments.get('--loop'))):
            update_store(arguments.get('<storenum>'), 1)

    if arguments.get('exit_store'):
        for i in range(int(arguments.get('--loop'))):
            update_store(arguments.get('<storenum>'), -1)

    if arguments.get('store_details'):
        store_details(arguments.get('<storenum>'))

    if arguments.get('store_counts'):
        store_counts()

    if arguments.get('reset'):
        store_data = {
            'count': 0,
            'timestamp': firestore.SERVER_TIMESTAMP
        }
        update_collection(db.collection(u'stores'),2000, store_data)

        delete_collection(db.collection(u'details'),2000)

        print("Store Counts")
        store_counts()

    if arguments.get('populate'):
        for i in range(10):
            for x in range((i + 10) * 4):
                update_store(str(i), 1)
                #print (x)
                
            for y in range((i + 10) * 3):
                update_store(str(i), -1)
                #print (y)
    
    if arguments.get('create'):
        create_stores(10)















