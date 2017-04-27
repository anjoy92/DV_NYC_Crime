import os
import json
from bson import ObjectId

APP_ROOT = os.path.dirname(os.path.abspath(__file__))   # refers to application_top
APP_STATIC = os.path.join(APP_ROOT, 'config.json')
with open(APP_STATIC) as f:
    config = json.load(f)


def set_query2_params(restaurant_id):
   query2['_id'] = ObjectId(restaurant_id)

# get restaurant location from restaurant ID
query2 = {
       '_id': ''
}

def set_query1_params(coord,lowerbound, upperbound, crime_type):
    query1['location']['$near']['$geometry']['coordinates'] = coord
    query1['CMPLNT_FR_TM']['$lte'] = upperbound
    query1['CMPLNT_FR_TM']['$gte'] = lowerbound
    query1['OFNS_DESC']['$in'] = crime_type

query1 = {
        "location" :
            {
                "$near" :
                    {
                        "$geometry":
                            {
                            "type": "Point",
                            "coordinates": ""
                            },
                        "$minDistance": 0,
                        "$maxDistance": config["D"]
                    }
            },
            "CMPLNT_FR_TM":
            {
                "$lte": "",
                "$gte": ""
            },
            "OFNS_DESC":
            {
                "$in": ""
            }

    }