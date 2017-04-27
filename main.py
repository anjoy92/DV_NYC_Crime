from flask import Flask, jsonify, url_for, redirect, request, render_template
from flask_cors import CORS, cross_origin
from flask_pymongo import PyMongo
import os
from flask import send_file
import json
from flask_restful import reqparse
import queries
import pandas as pd
import HTMLParser
import pysal
import numpy as np
from bson import ObjectId

app = Flask(__name__)
CORS(app)
app.config["MONGO_DBNAME"] = "nyctest"
mongo = PyMongo(app, config_prefix='MONGO')
APP_URL = "http://127.0.0.1:5000"
APP_ROOT = os.path.dirname(os.path.abspath(__file__))  # refers to application_top
APP_STATIC = os.path.join(APP_ROOT, 'config.json')
with open(APP_STATIC) as f:
    config = json.load(f)

# print app.root_path+'\\templates\\resources\image\stars.png'

def parse_arg_from_requests(arg, **kwargs):
    parse = reqparse.RequestParser()
    parse.add_argument(arg, **kwargs)
    args = parse.parse_args()
    return args[arg]

@app.route('/')
def hello_world():
    return render_template('home.html')

@app.route('/home2')
def hello_world2():
    return render_template('home2.html')

@app.route('/getRestaurantHeatMap', methods=['POST'])
def getRestaurantHeatMap():
    print "Loading Heat Map ...."
    data = request.get_data()
    obj = json.loads(data)
    placeId = obj['restaurantId']
    temp = {'Monday': 1, 'Tuesday': 2, 'Wednesday': 3, 'Thursday': 4, 'Friday': 5, 'Saturday': 6}
    place = mongo.db.restaurantsNew
    dataCur = place.find({'_id': ObjectId(placeId)},
                         {'crimeCount': 1, 'popular': 1, 'maxCountCrime': 1, 'maxCountPopular': 1})
    popularTime = []
    crimeData = []
    data = {}
    for i in dataCur:
        data = i
    for obj in data['popular']:
        for ctr in range(0, 18):
            dict = {}
            # print float(data['popular'][obj][ctr]  / data['maxCountPopular']),data['popular'][obj][ctr],data['maxCountPopular']
            dict['value'] = data['popular'][obj][ctr] * 100 / data['maxCountPopular']
            dict['hour'] = ctr + 1
            dict['day'] = temp[obj]
            # print dict
            popularTime.append(dict)
    for obj in data['crimeCount']:
        if obj != 'Sunday':
            if obj in data['crimeCount'].keys():
                for ctr in range(0, 18):
                    dict = {}
                    dict['hour'] = ctr + 1
                    dict['day'] = temp[str(obj)]
                    if str(ctr) in data['crimeCount'][obj].keys():
                        dict['value'] = data['crimeCount'][obj][str(ctr)] * 100 / data['maxCountCrime']
                    else:
                        dict['value'] = 0
                    crimeData.append(dict)
            else:
                for ctr in range(0, 18):
                    dict = {}
                    dict['hour'] = ctr + 1
                    dict['day'] = temp[str(obj)]
                    dict['value'] = 0
                    crimeData.append(dict)
    # print popularTime
    # print crimeData
    json_data = json.dumps({"popularTime": popularTime, "crimeData": crimeData})
    # print json_data
    return json_data


@app.route('/shobhit')
def get_data():
    friends = mongo.db.nyccrime
    output = []
    for s in friends.find({'location': {'$exists': 'true'}}).limit(5):
        output.append({'coordinates': s['location']['coordinates']})
    return jsonify({'result': output})

@app.route('/getRestaurantDetail', methods=['POST'])
def getRestaurantDetail():
    data = request.get_data()
    obj = json.loads(data)
    placeId = obj['resId']
    # print placeId
    places = mongo.db.restaurantsNew
    word_review = places.find({'_id': ObjectId(placeId)})
    data = {}
    for w in word_review:
        name = w['info']['name']
        rating = w['info']['rating']
        buildNo = ''
        if type(w['building']) is int:
            buildNo = str(int(w['building']))
        else:
            buildNo = str(w['building'])
        address = str(buildNo) + ' ' + str(w['street']) + ', ' + str(w['boro']) + ', NY, ' + str(
            int(w['zip']))
        cuisine = w['cuisine']
        url = w['info']['url']
        phone_str = str(w['phone'])
        phone = '(' + phone_str[0:3] + ') ' + phone_str[3:6] + '-' + phone_str[6:10]
        data['name'] = name
        data['address'] = address
        data['phone'] = phone
        data['url'] = url
        data['cuisine'] = cuisine
        data['rating'] = rating

    json_data = json.dumps(data)
    print json_data
    return json_data


@app.route('/sankeyData', methods=['POST'])
def get_sankeyData():
    print "Loading Sankey Chart ...."
    data = request.get_data()
    params = json.loads(data)

    resId = params['restaurantId']
    res = mongo.db.restaurantsNew

    options = params['options']

    options.append("OTHER NEARBY RESTAURANTS")

    police_list = []
    parking_list = []
    hospital_list = []

    restaurant_Collection = []

    res_crime_map = {}

    DIST = params['distance']
    crime_types = params['crime_types']

    restaurant = res.find({'_id': ObjectId(resId)}, {'info': 1})[0]
    restaurant_location = restaurant['info']['location']['coordinates']
    restaurant_name = restaurant['info']['name']

    query0 = {
        'info.location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': ''
            }
        }
    }
    query0['info.location']['$near']['$geometry']['coordinates'] = restaurant_location
    query0['info.location']['$near']['$maxDistance'] = DIST

    mongo.db.temp.remove()

    temp = mongo.db['temp']

    queryCrimeReduce = {
        'location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': 3000
            }
        }
    }
    queryCrimeReduce['location']['$near']['$geometry']['coordinates'] = restaurant_location
    temp.insert(mongo.db.nyccrime.find(queryCrimeReduce, {'OFNS_DESC': 1, 'location': 1, 'CMPLNT_NUM': 1}))

    temp.create_index([('location.coordinates', '2d')], name='location_2d', default_language='english')
    temp.create_index([('location', '2dsphere')], name='location.coordinates_2dsphere', default_language='english')

    restaurant_Collection_Iterator = res.find(query0, {'info.name': 1, 'info.location': 1})
    for cur_Res in restaurant_Collection_Iterator:
        restaurant_Collection.append(cur_Res)

    query1 = {
        'geometry': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': 3000
            }
        }
    }
    query2 = {
        'location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': 3000
            }
        }
    }
    query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
    pol_iterator = mongo.db.police.find(query1, {'geometry': 1, 'properties.name': 1, '_id': 1})
    for cur_Police in pol_iterator:
        police_list.append(cur_Police)

    park_iterator = mongo.db.parking.find(query1, {'geometry': 1, '_id': 1})
    for cur_parking in park_iterator:
        parking_list.append(cur_parking)

    query2['location']['$near']['$geometry']['coordinates'] = restaurant_location
    h_iterator = mongo.db.hospital.find(query2, {'location': 1, 'Name': 1})
    for cur_hospital in h_iterator:
        hospital_list.append(cur_hospital)
    police_crime_set = set([])
    sankey_lists = {'CRIMES AT ' + str(restaurant_name).upper(): {}, 'POLICE PRECINCTS': {}, 'PARKING': {},
                    'HOSPITALS': {}, 'OTHER NEARBY RESTAURANTS': {},
                    'CRIMES NEAR' + str(restaurant_name).upper(): {}}
    options.append('CRIMES AT ' + str(restaurant_name).upper())

    queryCrimeTemp = {
        'location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': 500
            }
        },
        'OFNS_DESC':
            {
                "$eq": ""
            }
    }
    crime_type_count_police = {}
    for cur_crime_Type in crime_types:
        crime_count = 0
        for cur in police_list:
            cur_location = cur['geometry']['coordinates']
            queryCrimeTemp['location']['$near']['$geometry']['coordinates'] = cur_location
            queryCrimeTemp['OFNS_DESC'] = cur_crime_Type
            crime_iterator = mongo.db.temp.find(queryCrimeTemp, {'_id': 0, 'CMPLNT_NUM': 1})
            # print cur_crime_Type
            for crime in crime_iterator:
                crimeId = crime['CMPLNT_NUM']
                if crimeId not in police_crime_set:
                    police_crime_set.add(crimeId)
                    crime_count += 1
        crime_type_count_police[cur_crime_Type] = crime_type_count_police.get(cur_crime_Type, 0) + crime_count
    sankey_lists['POLICE PRECINCTS'] = crime_type_count_police

    hospital_crime_set = set([])

    crime_type_count_hospital = {}
    for cur_crime_Type in crime_types:
        crime_count = 0
        for cur in hospital_list:
            cur_location = cur['location']['coordinates']
            queryCrimeTemp['location']['$near']['$geometry']['coordinates'] = cur_location
            queryCrimeTemp['OFNS_DESC'] = cur_crime_Type
            crime_iterator = mongo.db.temp.find(queryCrimeTemp, {'_id': 0, 'CMPLNT_NUM': 1})
            # print cur_crime_Type
            for crime in crime_iterator:
                crimeId = crime['CMPLNT_NUM']
                if crimeId not in hospital_crime_set and crimeId not in police_crime_set:
                    hospital_crime_set.add(crimeId)
                    crime_count += 1
        crime_type_count_hospital[cur_crime_Type] = crime_type_count_hospital.get(cur_crime_Type, 0) + crime_count
    sankey_lists['HOSPITALS'] = crime_type_count_hospital

    parking_crime_set = set([])

    crime_type_count_parking = {}
    for cur_crime_Type in crime_types:
        crime_count = 0
        for cur in parking_list:
            # print cur
            cur_location = cur['geometry']['coordinates']
            queryCrimeTemp['location']['$near']['$geometry']['coordinates'] = cur_location
            queryCrimeTemp['OFNS_DESC'] = cur_crime_Type
            crime_iterator = mongo.db.temp.find(queryCrimeTemp, {'_id': 0, 'CMPLNT_NUM': 1})
            for crime in crime_iterator:
                crimeId = crime['CMPLNT_NUM']
                if crimeId not in parking_crime_set and crimeId not in hospital_crime_set and crimeId not in police_crime_set:
                    parking_crime_set.add(crimeId)
                    crime_count += 1
        crime_type_count_parking[cur_crime_Type] = crime_type_count_parking.get(cur_crime_Type, 0) + crime_count
    sankey_lists['PARKING'] = crime_type_count_parking

    oherRes_crime_set = set([])
    crime_type_count_other = {}
    for cur_crime_Type in crime_types:
        crime_count = 0
        for cur in restaurant_Collection:
            cur_location = cur['info']['location']['coordinates']
            queryCrimeTemp['location']['$near']['$geometry']['coordinates'] = cur_location
            queryCrimeTemp['OFNS_DESC'] = cur_crime_Type
            crime_iterator = mongo.db.temp.find(queryCrimeTemp, {'_id': 0, 'CMPLNT_NUM': 1})
            for crime in crime_iterator:
                crimeId = crime['CMPLNT_NUM']
                if crimeId not in oherRes_crime_set and crimeId not in parking_crime_set and crimeId not in hospital_crime_set and crimeId not in police_crime_set:
                    oherRes_crime_set.add(crimeId)
                    crime_count += 1
        crime_type_count_other[cur_crime_Type] = crime_type_count_other.get(cur_crime_Type, 0) + crime_count
    sankey_lists['OTHER NEARBY RESTAURANTS'] = crime_type_count_other

    for cur_crime_Type in crime_types:
        queryForSelf = {
            'location': {
                '$near': {
                    '$geometry': {
                        'type': 'Point',
                        'coordinates': ''
                    }, '$minDistance': 0, '$maxDistance': 3000
                }
            },
            'OFNS_DESC':
                {
                    "$eq": ""
                }
        }

        queryForSelf['location']['$near']['$geometry']['coordinates'] = restaurant_location
        queryForSelf['OFNS_DESC'] = cur_crime_Type
        crime_iterator = mongo.db.temp.find(queryForSelf, {'_id': 0, 'CMPLNT_NUM': 1})
        crime_count = 0
        # print cur_crime_Type
        for crime in crime_iterator:
            crime_count += 1
        # print crime_count
        if cur_crime_Type in sankey_lists['PARKING'].keys():
            crime_count -= sankey_lists['PARKING'][cur_crime_Type]
        if cur_crime_Type in sankey_lists['POLICE PRECINCTS'].keys():
            crime_count -= sankey_lists['POLICE PRECINCTS'][cur_crime_Type]
        if cur_crime_Type in sankey_lists['HOSPITALS'].keys():
            crime_count -= sankey_lists['HOSPITALS'][cur_crime_Type]
        if cur_crime_Type in sankey_lists['OTHER NEARBY RESTAURANTS'].keys():
            crime_count -= sankey_lists['OTHER NEARBY RESTAURANTS'][cur_crime_Type]
        # print crime_count
        res_crime_map[cur_crime_Type] = crime_count

    temp.remove()

    for j in res_crime_map.keys():
        sankey_lists['CRIMES AT ' + str(restaurant_name).upper()][j] = res_crime_map[j]

    print sankey_lists

    nodes = []

    selfdict = {"name": 'CRIMES NEAR ' + str(restaurant_name).upper()}
    nodes.append(selfdict)

    for crime_type in crime_types:
        nodes.append({"name": crime_type})

    for option in options:
        nodes.append({"name": option})

    output = {}
    links = []

    for type in crime_types:
        for option in options:
            if type not in output:
                output[type] = {}
            if option not in output[type]:
                output[type][option] = 0
            if type in sankey_lists[option]:
                output[type][option] = sankey_lists[option][type]

    for type in crime_types:
        flg = 0
        for option in options:
            if output[type][option] == 0:
                flg += 1
        if flg == len(options):
            temp = {}
            temp['name'] = type
            nodes.remove(temp)
            output.pop(type, None)
            crime_types.remove(type)

    nodesDict = {}
    for i in range(len(nodes)):
        nodesDict[nodes[i]['name']] = i

    for type in crime_types:
        for option in options:
            temp = {}
            if output[type][option] > 0:
                temp['source'] = nodesDict[type]
                temp['target'] = nodesDict[option]
                temp['value'] = output[type][option]
                links.append(temp)

    print options

    for option in options:
        sum = 0
        for type in crime_types:
            sum += output[type][option]
        temp = {}
        temp['source'] = nodesDict[option]
        temp['target'] = nodesDict['CRIMES NEAR ' + str(restaurant_name).upper()]
        temp['value'] = sum
        links.append(temp)
    print links
    # print links

    result = {}
    result['nodes'] = nodes
    result['links'] = links
    # print nodes
    print result
    return json.dumps(result)

@app.route('/getWordCloudData', methods=['POST'])
def getWordCloudData():
    data = request.get_data()
    obj = json.loads(data)
    placeId = obj['placeId']
    # print placeId
    places = mongo.db.restaurantsNew
    word_review = places.find({'_id': ObjectId(placeId)}, {'_id': 1, 'wordCloud': 1})
    data = []
    for w in word_review:
        data = w['wordCloud']
    json_data = json.dumps(data)
    # print json_data
    return json_data


@app.route('/linedataMihir', methods=['POST'])
def get_line_data_mihir():
    data = request.get_data()
    params = json.loads(data)

    option_list = params['options']
    restaurant = mongo.db.restaurantsNew.find({})

    rating_list = []
    police_list = []
    parking_list = []
    hospital_list = []
    crime_list = []
    inspection_list = []
    injury_list = []

    for i in restaurant:
        rating_list.append(i['info']['rating'])

    DIST = params['Dist']

    query0 = {
        'info.location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': ''
            }
        }
    }
    query0['info.location']['$near']['$geometry']['coordinates'] = params['coordinates']
    query0['info.location']['$near']['$maxDistance'] = DIST

    restaurant = mongo.db.restaurantsNew.find(query0)
    restaurant_ids = []
    for r in restaurant:
        restaurant_ids.append(r['_id'])

    result = []

    query1 = {
        'geometry': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': DIST
            }
        }
    }
    query2 = {
        'location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': DIST
            }
        }
    }

    for restaurant in restaurant_ids:
        temp = {}
        queries.set_query2_params(restaurant)
        r = mongo.db.restaurantsNew.find(queries.query2)[0]
        restaurant_location = r['info']['location']['coordinates']

        query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
        pol_count = mongo.db.police.find(query1).count()
        police_list.append(pol_count)

        query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
        park_count = mongo.db.parking.find(query1).count()
        parking_list.append(park_count)

        query2['location']['$near']['$geometry']['coordinates'] = restaurant_location
        h_count = mongo.db.hospital.find(query2).count()
        hospital_list.append(h_count)

        injury_list.append(r['injuries'])

        c_count = 0
        queries.set_query2_params(restaurant)
        crime_counts = r['crime']
        for count in crime_counts:
            if count['id'] in params['crime_types']:
                c_count += count['value']
        crime_list.append(c_count)
        scores = []

        for inspection in r['inspection_list']:
            if 'SCORE' in inspection.keys() and (str(inspection['SCORE'])).isdigit():
                scores.append(int(inspection['SCORE']))
                inspection_list.append(sum(scores) / float(len(scores)))

        temp['id'] = str(ObjectId(r['_id']))
        temp['name'] = r['name']
        temp['image'] = r['info']['image_url']
        temp['police_actual'] = pol_count
        temp['hospital_actual'] = h_count
        temp['parking_actual'] = park_count
        temp['crime_actual'] = c_count
        temp['injury_actual'] = r['injuries']
        temp['inspection_score_actual'] = sum(scores) / float(len(scores))
        temp['rating_actual'] = r['info']['rating']
        result.append(temp)

    # print rating_list

    norm_rating_list = [100 * float(i) / max(rating_list) for i in rating_list]
    norm_police_list = [100 * float(i) / max(police_list) for i in police_list]
    norm_parking_list = [100 * float(i) / max(parking_list) for i in parking_list]
    norm_hospital_list = [100 * float(i) / max(hospital_list) for i in hospital_list]
    norm_crime_list = [100 * float(i) / max(crime_list) for i in crime_list]
    norm_inspection_list = [100 * float(i) / max(inspection_list) for i in inspection_list]
    norm_injury_list = [100 * float(i) / max(injury_list) for i in injury_list]

    norm_net_list = [0] * len(norm_crime_list)

    for i in range(len(norm_crime_list)):
        norm_net_list[i] = 0
        number = 0
        if "rating" in option_list:
            norm_net_list[i] += norm_rating_list[i]
            number += 1
        if "crime" in option_list:
            norm_net_list[i] -= norm_crime_list[i]
            number += 1
        if "hospital" in option_list:
            norm_net_list[i] += norm_hospital_list[i]
            number += 1
        if "parking" in option_list:
            norm_net_list[i] += norm_parking_list[i]
            number += 1
        if "police" in option_list:
            norm_net_list[i] += norm_police_list[i]
            number += 1
        if "injury" in option_list:
            norm_net_list[i] -= norm_injury_list[i]
            number += 1
        if "inspection_score" in option_list:
            norm_net_list[i] += norm_inspection_list[i]
            number += 1
        norm_net_list[i] /= number

    k = 0
    for o in result:
        # print option_list
        if "rating" in option_list:
            o['rating'] = norm_rating_list[k]
            o['panel_rating'] = ["Rating", rating_list[k]]
        if "crime" in option_list:
            o['crime'] = norm_crime_list[k]
            o['panel_crime'] = ["Crime", crime_list[k]]
        if "hospital" in option_list:
            o['hospital'] = norm_hospital_list[k]
            o['panel_hospital'] = ["Hospital", hospital_list[k]]
        if "parking" in option_list:
            o['parking'] = norm_parking_list[k]
            o['panel_parking'] = ["Parking", parking_list[k]]
        if "injury" in option_list:
            o['injury'] = norm_injury_list[k]
            o['panel_injury'] = ["Injury", injury_list[k]]
        if "police" in option_list:
            o['police'] = norm_police_list[k]
            o['panel_police'] = ["Police", police_list[k]]
        if "inspection_score" in option_list:
            o['inspection_score'] = norm_inspection_list[k]
            o['panel_inspection_score'] = ["Inspection Score", float("{0:.2f}".format(inspection_list[k]))]
        o['net'] = norm_net_list[k]
        o['panel_net'] = ["Net", norm_net_list[k]]
        k += 1

    # k = 0
    # for o in result:
    #     o['police'] = norm_police_list[k]
    #     o['hospital'] = norm_hospital_list[k]
    #     o['parking'] = norm_parking_list[k]
    #     o['crime'] = norm_crime_list[k]
    #     o['net'] = norm_net_list[k]
    #     o['rating'] = norm_rating_list[k]
    #     o['inspection_score'] = norm_inspection_list[k]
    #     o['injury'] = norm_injury_list[k]
    #     k += 1

    final = {'radial': {}, 'line': {}}

    from operator import itemgetter
    top30 = sorted(result, key=itemgetter('net'), reverse=True)[0:30]
    print top30

    outputs = []
    k = 0
    for o in top30:
        output = {}
        output['id'] = o['id']
        output['name'] = o['name']
        output['crime'] = o['crime_actual']
        output['hospital'] = o['hospital_actual']
        output['parking'] = o['parking_actual']
        output['police'] = o['police_actual']
        output['inspection_score'] = o['inspection_score_actual']
        output['rating'] = o['rating_actual']
        output['injury'] = o['injury_actual']
        outputs.append(output)
        k += 1
    final['line'] = outputs
    final['radial'] = top30
    return json.dumps(final)



@app.route('/linedata', methods=['POST'])
def get_line():
    data = request.get_data()
    params = json.loads(data)

    option_list = params['options']
    print option_list

    # if "rating" in option_list:
    #     print "rating"
    #     # o['panel_rating'] = ["Rating", rating_list[k]]
    # if "crime" in option_list:
    #     print "crime"# o['panel_crime'] = ["Crime", crime_list[k]]
    # if "hospital" in option_list:
    #     print "hosp"# o['panel_hospital']=["Hospital",hospital_list[k]]
    # if "parking" in option_list:
    #     print "park"# o['panel_parking'] = ["Parking", parking_list[k]]
    # if "police" in option_list:
    #     print "poli"# o['panel_injury'] = ["Injury", parking_list[k]]
    # if "injury" in option_list:
    #     print "inju"# o['panel_police'] = ["Police", police_list[k]]
    # if "inspection_score" in option_list:
    #     print "ins"# o['panel_inspection_score'] = ["Inspection Score", police_list[k]]
    # # print params
    restaurant = mongo.db.restaurantsNew.find({})

    rating_list = []
    police_list = []
    parking_list = []
    hospital_list = []
    crime_list = []
    inspection_list = []
    injury_list = []
    image_urls = []

    for i in restaurant:
        rating_list.append(i['info']['rating'])

    DIST = params['Dist']

    query0 = {
        'info.location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': ''
            }
        }
    }
    query0['info.location']['$near']['$geometry']['coordinates'] = params['coordinates']
    query0['info.location']['$near']['$maxDistance'] = DIST

    restaurant = mongo.db.restaurantsNew.find(query0)
    restaurant_ids = []
    for r in restaurant:
        restaurant_ids.append(r['_id'])

    result = []

    query1 = {
        'geometry': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': 5000
            }
        }
    }
    query2 = {
        'location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': 5000
            }
        }
    }

    for restaurant in restaurant_ids:
        temp = {}
        queries.set_query2_params(restaurant)
        r = mongo.db.restaurantsNew.find(queries.query2)[0]
        restaurant_location = r['info']['location']['coordinates']

        query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
        pol_count = mongo.db.police.find(query1).count()
        police_list.append(pol_count)

        query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
        park_count = mongo.db.parking.find(query1).count()
        parking_list.append(park_count)

        query2['location']['$near']['$geometry']['coordinates'] = restaurant_location
        h_count = mongo.db.hospital.find(query2).count()
        hospital_list.append(h_count)

        injury_list.append(r['injuries'])

        c_count = 0
        queries.set_query2_params(restaurant)
        crime_counts = r['crime']  # count json
        for count in crime_counts:
            if count['id'] in params['crime_types']:
                c_count += count['value']
        crime_list.append(c_count)
        scores = []

        for inspection in r['inspection_list']:
            if 'SCORE' in inspection.keys() and (str(inspection['SCORE'])).isdigit():
                scores.append(int(inspection['SCORE']))
                inspection_list.append(sum(scores) / float(len(scores)))
        temp['id'] = str(ObjectId(r['_id']))
        temp['name'] = r['name']
        temp['image'] = r['info']['image_url']
        temp['lat']=r['info']['location']['coordinates'][1]
        temp['lng']=r['info']['location']['coordinates'][0]
        temp['crime_cnt']=c_count
        result.append(temp)

    norm_rating_list = [100 * float(i) / max(rating_list) for i in rating_list]
    norm_police_list = [100 * float(i) / max(police_list) for i in police_list]
    norm_parking_list = [100 * float(i) / max(parking_list) for i in parking_list]
    norm_hospital_list = [100 * float(i) / max(hospital_list) for i in hospital_list]
    norm_crime_list = [100 * float(i) / max(crime_list) for i in crime_list]
    norm_inspection_list = [100 * float(i) / max(inspection_list) for i in inspection_list]
    norm_injury_list = [100 * float(i) / max(injury_list) for i in injury_list]

    norm_net_list = [0] * len(norm_crime_list)

    # # TODO logic
    # for i in range(len(norm_crime_list)):
    #     norm_net_list[i] = (norm_rating_list[i] - norm_crime_list[i] + norm_hospital_list[i] + norm_parking_list[i] +
    #                         norm_police_list[i]) - norm_injury_list[i] + norm_inspection_list[i] / 7

    for i in range(len(norm_crime_list)):
        norm_net_list[i] = 0
        number = 0
        if "rating" in option_list:
            norm_net_list[i] += norm_rating_list[i]
            number += 1
        if "crime" in option_list:
            norm_net_list[i] -= norm_crime_list[i]
            number += 1
        if "hospital" in option_list:
            norm_net_list[i] += norm_hospital_list[i]
            number += 1
        if "parking" in option_list:
            norm_net_list[i] += norm_parking_list[i]
            number += 1
        if "police" in option_list:
            norm_net_list[i] += norm_police_list[i]
            number += 1
        if "injury" in option_list:
            norm_net_list[i] -= norm_injury_list[i]
            number += 1
        if "inspection_score" in option_list:
            norm_net_list[i] += norm_inspection_list[i]
            number += 1
        norm_net_list[i] /= number

    k = 0
    for o in result:
        if "rating" in option_list:
            o['rating'] = norm_rating_list[k]
            o['panel_rating'] = ["Rating", rating_list[k]]
        if "crime" in option_list:
            o['crime'] = norm_crime_list[k]
            o['panel_crime'] = ["Crime", crime_list[k]]
        if "hospital" in option_list:
            o['hospital'] = norm_hospital_list[k]
            o['panel_hospital']=["Hospital",hospital_list[k]]
        if "parking" in option_list:
            o['parking'] = norm_parking_list[k]
            o['panel_parking'] = ["Parking", parking_list[k]]
        if "injury" in option_list:
            o['injury'] = norm_injury_list[k]
            o['panel_injury'] = ["Injury", parking_list[k]]
        if "police" in option_list:
            o['police'] = norm_police_list[k]
            o['panel_police'] = ["Police", police_list[k]]
        if "inspection_score" in option_list:
            o['inspection_score'] = norm_inspection_list[k]
            o['panel_inspection_score'] = ["Inspection Score", float("{0:.2f}".format(inspection_list[k]))  ]

        o['net'] = norm_net_list[k]
        o['panel_net'] = ["Net", norm_net_list[k]]

        k += 1
    #result = result[30:60]
    from operator import itemgetter
    top30 = sorted(result, key=itemgetter('net'), reverse=True)[0:30]
    print top30
    return json.dumps(top30)

@app.route('/placesOfInterest/')
def get_Place_data():
    friends = mongo.db.places
    output = []
    for s in friends.find():
        temp = {}
        temp['name'] = s['name']
        temp['rating'] = s['rating']
        temp['formatted_address'] = s['formatted_address']
        temp['photo_reference'] = 'https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference=' + \
                                  s['photos'][0]['photo_reference'] + '&key=AIzaSyA_H-WIDxH1TiNw4SfAjdUaP0RTAdag_fY'
        output.append(temp)
    # print jsonify({'result' : output})
    return jsonify({'result': output})


@app.route('/getCrimeFIlter/')
def get_Filter_Crime_data():
    print jsonify({'result': config['result']})
    return jsonify({'result': config['result']})


@app.route('/image/stars/')
def get_Image():
    return send_file(app.root_path + '/templates/resources/image/stars.png', mimetype='image/gif')


@app.route('/image/pin2/')
def get_Image_Pin2():
    return send_file(app.root_path + '/templates/resources/image/pin3.png', mimetype='image/png')


@app.route('/image/pin/')
def get_Image_Pin():
    return send_file(app.root_path + '/templates/resources/image/pin4.png', mimetype='image/png')

@app.route('/image/pin3/')
def get_Image_Pin23():
    return send_file(app.root_path + '/templates/resources/image/pin.png', mimetype='image/png')


@app.route('/image/hosp1/')
def get_Image_hosp1():
    return send_file(app.root_path + '/templates/resources/image/hosp1.png', mimetype='image/png')


@app.route('/image/hosp2/')
def get_Image_hosp2():
    return send_file(app.root_path + '/templates/resources/image/hosp2.png', mimetype='image/png')


@app.route('/image/hosp3/')
def get_Image_hosp3():
    return send_file(app.root_path + '/templates/resources/image/hosp3.png', mimetype='image/png')


@app.route('/image/rail1/')
def get_Image_rail1():
    return send_file(app.root_path + '/templates/resources/image/rail1.png', mimetype='image/png')


@app.route('/image/rail2/')
def get_Image_rail2():
    return send_file(app.root_path + '/templates/resources/image/rail2.png', mimetype='image/png')


@app.route('/image/rail3/')
def get_Image_rail3():
    return send_file(app.root_path + '/templates/resources/image/rail3.png', mimetype='image/png')


@app.route('/image/park1/')
def get_Image_park1():
    return send_file(app.root_path + '/templates/resources/image/park1.png', mimetype='image/png')


@app.route('/image/park2/')
def get_Image_park2():
    return send_file(app.root_path + '/templates/resources/image/park2.png', mimetype='image/png')


@app.route('/image/park3/')
def get_Image_park3():
    return send_file(app.root_path + '/templates/resources/image/park3.png', mimetype='image/png')


@app.route('/image/bus1/')
def get_Image_bus1():
    return send_file(app.root_path + '/templates/resources/image/bus1.png', mimetype='image/png')


@app.route('/image/bus2/')
def get_Image_bus2():
    return send_file(app.root_path + '/templates/resources/image/bus2.png', mimetype='image/png')


@app.route('/image/bus3/')
def get_Image_bus3():
    return send_file(app.root_path + '/templates/resources/image/bus3.png', mimetype='image/png')


@app.route('/image/police1/')
def get_Image_police1():
    return send_file(app.root_path + '/templates/resources/image/police1.png', mimetype='image/png')


@app.route('/image/police2/')
def get_Image_police2():
    return send_file(app.root_path + '/templates/resources/image/police2.png', mimetype='image/png')


@app.route('/image/police3/')
def get_Image_police3():
    return send_file(app.root_path + '/templates/resources/image/police3.png', mimetype='image/png')

@app.route('/image/pin5/')
def get_Image_Pin5():
   return send_file(app.root_path + '/templates/resources/image/pin5.png', mimetype='image/png')

@app.route('/getCrimeFilterData', methods=['POST'])
def getCrimeFilterData():
    data = request.get_data()
    obj = json.loads(data)

    places = mongo.db.places
    place_list = []
    rating_list = []
    coord_list = []
    crime_count_list = []

    output = places.find({}, {'place_id': 1})
    for i in output:
        place_list.append(i['place_id'])

    num_of_places = len(place_list)

    pop_unorm_list = [0] * num_of_places
    pop_list = [0] * num_of_places
    if (obj['review'] == 'true'):
        consider_popularity = True
    else:
        consider_popularity = False

    if consider_popularity == True:
        output = places.find({}, {'popularity': 1})
        k = 0
        for i in output:
            pop_unorm_list[k] = i['popularity']
            k = k + 1
        pop_list = [config['PopularityWeight'] * float(i) / max(pop_unorm_list) for i in pop_unorm_list]

    output = places.find({}, {'rating': 1})
    for i in output:
        rating_list.append(i['rating'])

    for i in range(len(pop_list)):
        pop_list[i] = pop_list[i] + rating_list[i]

    output = places.find({}, {'geometry': 1})
    for i in output:
        temp = [i['geometry']['location']['lat'], i['geometry']['location']['lng']]
        coord_list.append(temp)

    bins = config['BINS']
    ranks = range(1, bins + 1)
    time_upper_bound = str(obj['endTime']).zfill(2) + ":00:00"
    time_lower_bound = str(obj['startTime']).zfill(2) + ":00:00"
    print time_lower_bound, time_upper_bound
    if (time_upper_bound == '00:00:00'):
        time_upper_bound = '24:00:00'
    crime_types1 = obj['crime']
    # print crime_types1
    crime_types = []
    for s in crime_types1:
        crime_types.append(HTMLParser.HTMLParser().unescape(s))

    # print crime_types
    for loc in coord_list:
        POI_coord = []
        POI_coord.append(loc[1])
        POI_coord.append(loc[0])

        queries.set_query1_params(POI_coord, time_lower_bound, time_upper_bound, crime_types)
        crime_count_list.append(mongo.db.nyccrime.find(queries.query1).count())

    crime_count_list = [5 * float(i) / max(crime_count_list) for i in crime_count_list]
    net_count = []

    for i in range(len(place_list)):
        net_count.append(pop_list[i] - crime_count_list[i])

    crime_ranking = pd.qcut(crime_count_list, bins, labels=ranks)
    pop_ranking = pd.qcut(pop_list, bins, labels=ranks)

    data = []
    for i in range(len(place_list)):
        temp = {}
        temp['place_id'] = str(place_list[i])
        temp['pop_rank'] = pop_ranking[i]
        temp['crime_rank'] = bins - crime_ranking[i] + 1
        temp['net_count'] = net_count[i]
        data.append(temp)

    data_sorted = sorted(data, key=lambda k: k['net_count'], reverse=True)

    for t in data_sorted:
        friends = mongo.db.places
        for s in friends.find():
            if (s['place_id'] == t['place_id']):
                t['name'] = str(s['name'])
                t['rating'] = s['rating']
                t['formatted_address'] = str(s['formatted_address'])
                t['photo_reference'] = str(
                    'https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference=' + \
                    s['photos'][0]['photo_reference'] + '&key=AIzaSyA_H-WIDxH1TiNw4SfAjdUaP0RTAdag_fY')
                t['lat'] = s['geometry']['location']['lat']
                t['lng'] = s['geometry']['location']['lng']
    json_data = json.dumps(str(data_sorted))
    print data_sorted
    return json.dumps({'result': str(data_sorted).decode("utf-8")})

@app.route('/getRestaurantFromPlace', methods=['POST'])
def getRestaurantFromPlace():
    data = request.get_data()
    params = json.loads(data)

    option_list = params['options']
    restaurant = mongo.db.restaurantsNew.find({})

    rating_list = []
    police_list = []
    parking_list = []
    hospital_list = []
    crime_list = []
    inspection_list = []
    injury_list = []

    for i in restaurant:
        rating_list.append(i['info']['rating'])

    DIST = params['Dist']

    query0 = {
        'info.location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': ''
            }
        }
    }
    query0['info.location']['$near']['$geometry']['coordinates'] = params['coordinates']
    query0['info.location']['$near']['$maxDistance'] = DIST

    restaurant = mongo.db.restaurantsNew.find(query0)
    restaurant_ids = []
    for r in restaurant:
        restaurant_ids.append(r['_id'])

    result = []

    query1 = {
        'geometry': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': DIST
            }
        }
    }
    query2 = {
        'location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': DIST
            }
        }
    }

    for restaurant in restaurant_ids:
        temp = {}
        queries.set_query2_params(restaurant)
        r = mongo.db.restaurantsNew.find(queries.query2)[0]
        restaurant_location = r['info']['location']['coordinates']

        query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
        pol_count = mongo.db.police.find(query1).count()
        police_list.append(pol_count)

        query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
        park_count = mongo.db.parking.find(query1).count()
        parking_list.append(park_count)

        query2['location']['$near']['$geometry']['coordinates'] = restaurant_location
        h_count = mongo.db.hospital.find(query2).count()
        hospital_list.append(h_count)

        injury_list.append(r['injuries'])

        c_count = 0
        queries.set_query2_params(restaurant)
        crime_counts = r['crime']
        for count in crime_counts:
            if count['id'] in params['crime_types']:
                c_count += count['value']
        crime_list.append(c_count)
        scores = []

        for inspection in r['inspection_list']:
            if 'SCORE' in inspection.keys() and (str(inspection['SCORE'])).isdigit():
                scores.append(int(inspection['SCORE']))
                inspection_list.append(sum(scores) / float(len(scores)))

        temp['id'] = str(ObjectId(r['_id']))
        temp['name'] = r['name']
        temp['image'] = r['info']['image_url']
        temp['police_actual'] = pol_count
        temp['hospital_actual'] = h_count
        temp['parking_actual'] = park_count
        temp['crime_actual'] = c_count
        temp['injury_actual'] = r['injuries']
        temp['inspection_score_actual'] = sum(scores) / float(len(scores))
        temp['rating_actual'] = r['info']['rating']
        buildNo = ''
        if type(r['building']) is int:
            buildNo = str(int(r['building']))
        else:
            buildNo = str(r['building'])
        temp['formatted_address'] = str(buildNo) + ' ' + str(r['street']) + ', ' + str(r['boro']) + ', NY, ' + str(
            int(r['zip']))
        temp['lat'] = r['info']['location']['coordinates'][1]
        temp['lng'] = r['info']['location']['coordinates'][0]

        result.append(temp)

    # print rating_list

    norm_rating_list = [100 * float(i) / max(rating_list) for i in rating_list]
    norm_police_list = [100 * float(i) / max(police_list) for i in police_list]
    norm_parking_list = [100 * float(i) / max(parking_list) for i in parking_list]
    norm_hospital_list = [100 * float(i) / max(hospital_list) for i in hospital_list]
    norm_crime_list = [100 * float(i) / max(crime_list) for i in crime_list]
    norm_inspection_list = [100 * float(i) / max(inspection_list) for i in inspection_list]
    norm_injury_list = [100 * float(i) / max(injury_list) for i in injury_list]

    norm_net_list = [0] * len(norm_crime_list)

    for i in range(len(norm_crime_list)):
        norm_net_list[i] = 0
        number = 0
        if "rating" in option_list:
            norm_net_list[i] += norm_rating_list[i]
            number += 1
        if "crime" in option_list:
            norm_net_list[i] -= norm_crime_list[i]
            number += 1
        if "hospital" in option_list:
            norm_net_list[i] += norm_hospital_list[i]
            number += 1
        if "parking" in option_list:
            norm_net_list[i] += norm_parking_list[i]
            number += 1
        if "police" in option_list:
            norm_net_list[i] += norm_police_list[i]
            number += 1
        if "injury" in option_list:
            norm_net_list[i] -= norm_injury_list[i]
            number += 1
        if "inspection_score" in option_list:
            norm_net_list[i] += norm_inspection_list[i]
            number += 1
        norm_net_list[i] /= number

    k = 0
    for o in result:
        # print option_list
        o['net'] = norm_net_list[k]
        o['panel_net'] = ["Net", norm_net_list[k]]
        k += 1

    from operator import itemgetter
    top30 = sorted(result, key=itemgetter('net'), reverse=True)[0:30]
    #print top30

    outputs = []
    k = 0
    for o in top30:
        output = {}
        output['place_id'] = o['id']
        output['name'] = o['name']
        output['rating'] = o['rating_actual']
        output['formatted_address'] = o['formatted_address']
        output['photo_reference'] = o['image']
        output['lat'] = o['lat']
        output['lng'] = o['lng']
        outputs.append(output)
        k += 1
    #print output
    print len(outputs)
    return json.dumps({'result': (outputs)})


@app.route('/getBubbleData', methods=['POST'])
def get_bubble_data():
    data = request.get_data()
    params = json.loads(data)
    print "Loading Bubble Chart ...."
    crime = mongo.db.nyccrime
    restaurant = mongo.db.restaurantsNew

    restaurant_id = params['restaurantId']
    crime_types = params['crime_types']

    queries.set_query2_params(restaurant_id)
    r = restaurant.find(queries.query2)[0]
    restaurant_location = r['info']['location']['coordinates']
    # print restaurant_location
    crime_counts = {}

    for c in crime_types:
        queries.set_query1_params(restaurant_location, "00:00:00", "24:00:00", [c])
        crime_counts[c] = crime.find(queries.query1).count()
    return json.dumps(crime_counts)





@app.route('/streamdata', methods=['GET'])
def stream_data():
    places = mongo.db.places
    crimes = mongo.db.nyccrime

    query1 = {
        "location":
            {
                "$near":
                    {
                        "$geometry":
                            {
                                "type": "Point",
                                "coordinates": ""
                            },
                        "$minDistance": 0,
                        "$maxDistance": 500
                    }
            },
        "CMPLNT_FR_DT":
            {
                "$lte": "",
                "$gte": ""
            }
    }

    import datetime,time
    epoch = datetime.datetime.utcfromtimestamp(0)

    def unix_time_millis(dt):
        return (dt - epoch).total_seconds() * 1000.0

    #months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    months = ['01', '02', '03', '04', '05', '06', '07']
    output = []
    for place in places.find():
        result = {"key": "", "values": []}
        p_id = str(place['place_id'])
        #print p_id
        p_name = place['name']
        p_location = []
        p_location.append(place['geometry']['location']['lng'])
        p_location.append(place['geometry']['location']['lat'])
        #print p_location
        result['placeId'] = p_id
        result["key"] = p_name

        query1['location']['$near']['$geometry']['coordinates'] = p_location
        ii=0
        dayTick=[]
        for kk in range(8):
            dayTick.append(int(time.mktime(datetime.datetime(2017, 4, 3+kk, 0, 0, 0, 0).timetuple())) * 1000)
        for month in months:
            temp = []
            query1['CMPLNT_FR_DT']['$lte'] = str(month) + '/31/2016'
            query1['CMPLNT_FR_DT']['$gte'] = str(month) + '/00/2016'
            #time1 = int(time.mktime(datetime.datetime(2016, int(month), 1, 0, 0, 0, 0).timetuple())) * 1000
            time1=dayTick[ii]
            #print time1
            temp.append(time1)
            temp.append(crimes.find(query1).count())
            result["values"].append(temp)
            ii=ii+1

        output.append(result)
        # for crime in crimes.find(query1):
        #     print crime['CMPLNT_FR_DT']
    print output
    return json.dumps(output)




# @app.route('/streamdata', methods=['GET'])
# def stream_data():
#     places = mongo.db.places
#     crimes = mongo.db.nyccrime
#     query1 = {
#         "location":
#             {
#                 "$near":
#                     {
#                         "$geometry":
#                             {
#                                 "type": "Point",
#                                 "coordinates": ""
#                             },
#                         "$minDistance": 0,
#                         "$maxDistance": 1000
#                     }
#             }  # ,
#         # "CMPLNT_FR_DT":
#         #     {
#         #         "$lte": "",
#         #         "$gte": ""
#         #     }
#     }
#
#     import datetime,time
#     epoch = datetime.datetime.utcfromtimestamp(0)
#
#     monday= int(time.mktime(datetime.datetime(2017, 4, 3, 0, 0, 0, 0).timetuple())) * 1000
#     tuesday=int(time.mktime(datetime.datetime(2017, 4, 4, 0, 0, 0, 0).timetuple())) * 1000
#     wednesday=int(time.mktime(datetime.datetime(2017, 4, 5, 0, 0, 0, 0).timetuple())) * 1000
#     thursday=int(time.mktime(datetime.datetime(2017, 4, 6, 0, 0, 0, 0).timetuple())) * 1000
#     friday=int(time.mktime(datetime.datetime(2017, 4, 7, 0, 0, 0, 0).timetuple())) * 1000
#     saturday=int(time.mktime(datetime.datetime(2017, 4, 8, 0, 0, 0, 0).timetuple())) * 1000
#     sunday=int(time.mktime(datetime.datetime(2017, 4, 9, 0, 0, 0, 0).timetuple())) * 1000
#     def unix_time_millis(dt):
#         return (dt - epoch).total_seconds() * 1000.0
#
#     months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
#     output = []
#
#     for place in places.find():
#         result = {"key": "", "values": []}
#         p_id = place['_id']
#         p_name = place['name']
#         p_location = []
#         p_location.append(place['geometry']['location']['lng'])
#         p_location.append(place['geometry']['location']['lat'])
#         print p_location
#
#         result["key"] = p_name
#
#         query1['location']['$near']['$geometry']['coordinates'] = p_location
#
#         temp = [[monday,0],[tuesday,0],[wednesday,0],[thursday,0], [friday, 0], [saturday, 0],[sunday,0]]
#         for crime in crimes.find(query1):
#             c_date = crime['CMPLNT_FR_DT']
#             month = c_date[0:2]
#             day = c_date[3:5]
#             year = c_date[6:]
#             # print c_date
#             # print month
#             # print day
#             # print year
#             # week = datetime.datetime(2012, 3, 23, 23, 24, 55, 173504)
#             week = datetime.datetime(int(year), int(month), int(day)).weekday()
#             # print c_date + str(week)
#
#             # print week
#             temp[week][1] += 1
#             # if athvadyu[week] in temp:
#             #     temp[athvadyu[week]] += 1
#             # else:
#             #     temp[athvadyu[week]] = 1
#
#         result["values"]=temp
#
#         # for month in months:
#         #     temp = []
#         #     query1['CMPLNT_FR_DT']['$lte'] = str(month) + '/31/2016'
#         #     query1['CMPLNT_FR_DT']['$gte'] = str(month) + '/00/2016'
#         #     time = int(datetime.datetime(2016, int(month), 1, 0, 0, 0, 0).strftime("%s")) * 1000
#         #     #print time
#         #     temp.append(time)
#         #     temp.append(crimes.find(query1).count())
#         #     result["values"].append(temp)
#
#         output.append(result)
#         # for crime in crimes.find(query1):
#         #     print crime['CMPLNT_FR_DT']
#     print output
#     return json.dumps(output)

@app.route('/placesHorizontal', methods=['POST'])
def places_horizontal():

    data = request.get_data()
    params = json.loads(data)

    places = mongo.db.places
    upper = params['time_upper_bound']
    lower = params['time_lower_bound']
    crime_types = params['crime_types']

    rating_list = []
    crime_list = []
    popularity_list = []

    for i in places.find():
        rating_list.append(i['rating'])

    result = []

    for place in places.find():
        temp = {}
        p_location = []
        p_location.append(place['geometry']['location']['lng'])
        p_location.append(place['geometry']['location']['lat'])

        queries.set_query1_params(p_location, lower, upper, crime_types)
        crime_list.append(mongo.db.nyccrime.find(queries.query1).count())

        popularity_list.append(place['popularity'])

        temp['id'] = place['place_id']
        temp['name'] = place['name']
        #print temp
        result.append(temp)

    k = 0
    for o in result:
        o['crime'] = crime_list[k]
        o['rating'] = rating_list[k]
        o['popularity'] = popularity_list[k]
        k += 1

    return json.dumps(result)


@app.route('/getDetailForPlacesOfInterest', methods=['POST'])
def getDetailForPlacesOfInterest():
    data = request.get_data()
    obj = json.loads(data)
    place_Id = obj['placeId']
    p = mongo.db.places.find({'place_id': place_Id})
    placeId = p[0]['id']

    print placeId
    friends = mongo.db.placesInfo
    temp = {}
    for s in friends.find({'id': placeId}):
        temp['name'] = s['name']
        temp['rating'] = s['rating']
        temp['formatted_address'] = s['formatted_address']
        temp['formatted_phone_number'] = s['formatted_phone_number']
        temp['website'] = s['website']
        keyword = ''
        for ty in s['types']:
            keyword += ty[0].upper() + ty[1:] + ', '
        temp['keyword'] = keyword[0:len(keyword) - 2]
        dict = {}
        for dt in s['opening_hours']['weekday_text']:
            date = str(dt.encode('utf8'))
            dict[date[0:date.find(': ')]] = date[date.find(': ') + 2:]
        convert = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5,
                   'Sunday': 6}
        for i in sorted(dict, key=convert.get):
            dict[i] = dict[i]
        temp['opening_hours'] = dict
        photo = {}
        tt = 0
        for dt in s['photos']:
            photo[
                'photo_' + str(tt)] = 'https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference=' + \
                                      dt[
                                          'photo_reference'] + '&key=AIzaSyA_H-WIDxH1TiNw4SfAjdUaP0RTAdag_fY'
            tt += 1
        # print photo
        temp['photo_reference'] = photo
        newsFin = []

        placeTb = mongo.db.places
        for cur in placeTb.find({'id': placeId}, {'news': 1}):
            for data in cur['news']:
                news = {}
                news['headline'] = data['headline']['main']
                news['url'] = data['web_url']
                news['content'] = data['snippet']
                newsFin.append(news)
        temp['newsArray'] = newsFin
    return json.dumps(temp)


@app.route('/getRoadData', methods=['POST'])
def getRoadData():
    print "IN ROAD"
    data = request.get_data()
    params = json.loads(data)

    print params
    query = {
        "geometry": {
            "$geoIntersects": {
                "$geometry": {
                    "type": "Polygon",
                    "coordinates": ""
                }
            }
        }
    }

    poly = [[[params['west'], params['south']],
             [params['west'], params['north']],
             [params['east'], params['north']],
             [params['east'], params['south']],
             [params['west'], params['south']]]]

    query['geometry']['$geoIntersects']['$geometry']['coordinates'] = poly

    roads = mongo.db.nycOSM.find(query)

    output = []
    crime_array = []
    crime_types = []
    for s in params['crime_types']:
        crime_types.append(HTMLParser.HTMLParser().unescape(s))
    for road in roads:
        sub_roads = road['crime_counts']
        sub_prop = road['properties']
        # print "ID = " + str(road['id'])
        for sub_road in sub_roads:
            temp = {}
            crime_counts = sub_road['count']
            count = 0
            for crime_count in crime_counts:
                if crime_count['id'] in crime_types:
                    count += crime_count['value']
            temp['source'] = sub_road['source']
            temp['destination'] = sub_road['destination']
            temp['count'] = count
            if 'name' in sub_prop.keys():
                temp['name'] = sub_prop['name']
            if 'highway' in sub_prop.keys():
                temp['highway'] = sub_prop['highway']
            temp['count'] = count
            crime_array.append(count)
            output.append(temp)

    st = pysal.esda.mapclassify.Natural_Breaks(crime_array, k=3)
    data2 = np.array(crime_array)
    pp = pysal.User_Defined.make(bins=st.bins)(data2)
    ind = 0
    for i in output:
        i['color'] = pp[ind] + 1
        ind = ind + 1

    ############################# HOSPITAL #########################


    hosps = mongo.db.hospital_new.find(query)
    output_hosp = []
    hosp_array = []
    for hosp in hosps:
        cnt = 0
        temp = {}
        for cty in crime_types:
            if cty in hosp['crimeCount'].keys():
                cnt = cnt + hosp['crimeCount'][cty]
        temp['count'] = cnt
        hosp_array.append(cnt)
        temp['lat'] = hosp['geometry']['coordinates'][1]
        temp['lng'] = hosp['geometry']['coordinates'][0]
        if 'name' in hosp['properties']:
            temp['name'] = hosp['properties']['name']
        if 'created' in hosp['properties']:
            temp['created'] = hosp['properties']['created']
        output_hosp.append(temp)

    hosp_array2 = []
    ho_crime = mongo.db.hospital_new.find()

    for hosp in ho_crime:
        cnt = 0
        for cty in crime_types:
            if cty in hosp['crimeCount'].keys():
                cnt = cnt + hosp['crimeCount'][cty]
        hosp_array2.append(cnt)

    st = pysal.esda.mapclassify.Natural_Breaks(hosp_array2, k=3)

    data2 = np.array(hosp_array)
    pp = pysal.User_Defined.make(bins=st.bins)(data2)
    ind = 0
    for i in output_hosp:
        i['color'] = pp[ind] + 1
        ind = ind + 1

    ############################# RAILWAY #########################


    rails = mongo.db.railway.find(query)
    output_rail = []
    rail_array = []
    for rail in rails:
        cnt = 0
        temp = {}
        for cty in crime_types:
            if cty in rail['crimeCount'].keys():
                cnt = cnt + rail['crimeCount'][cty]
        temp['count'] = cnt
        rail_array.append(cnt)
        temp['lat'] = rail['geometry']['coordinates'][1]
        temp['lng'] = rail['geometry']['coordinates'][0]
        if 'name' in rail['properties']:
            temp['name'] = rail['properties']['name']
        output_rail.append(temp)

    rail_array2 = []
    ra_crime = mongo.db.railway.find()

    for rail in ra_crime:
        cnt = 0
        for cty in crime_types:
            if cty in rail['crimeCount'].keys():
                cnt = cnt + rail['crimeCount'][cty]
        rail_array2.append(cnt)
    st = pysal.esda.mapclassify.Natural_Breaks(rail_array2, k=3)

    data2 = np.array(rail_array)
    pp = pysal.User_Defined.make(bins=st.bins)(data2)
    ind = 0
    for i in output_rail:
        i['color'] = pp[ind] + 1
        ind = ind + 1

    ############################# PARKING #########################


    rails = mongo.db.parking.find(query)
    output_park = []
    rail_array = []
    for rail in rails:
        cnt = 0
        temp = {}
        for cty in crime_types:
            if cty in rail['crimeCount'].keys():
                cnt = cnt + rail['crimeCount'][cty]
        temp['count'] = cnt
        rail_array.append(cnt)
        temp['lat'] = rail['geometry']['coordinates'][1]
        temp['lng'] = rail['geometry']['coordinates'][0]
        if 'name' in rail['properties']:
            temp['name'] = rail['properties']['name']
        output_park.append(temp)

    rail_array2 = []
    ra_crime = mongo.db.parking.find()

    for rail in ra_crime:
        cnt = 0
        for cty in crime_types:
            if cty in rail['crimeCount'].keys():
                cnt = cnt + rail['crimeCount'][cty]
        rail_array2.append(cnt)
    st = pysal.esda.mapclassify.Natural_Breaks(rail_array2, k=3)

    data2 = np.array(rail_array)
    pp = pysal.User_Defined.make(bins=st.bins)(data2)
    ind = 0
    for i in output_park:
        i['color'] = pp[ind] + 1
        ind = ind + 1

    ############################# BUS #########################


    rails = mongo.db.bus_stop.find(query)
    output_bus = []
    rail_array = []
    for rail in rails:
        cnt = 0
        temp = {}
        for cty in crime_types:
            if cty in rail['crimeCount'].keys():
                cnt = cnt + rail['crimeCount'][cty]
        temp['count'] = cnt
        rail_array.append(cnt)
        temp['lat'] = rail['geometry']['coordinates'][1]
        temp['lng'] = rail['geometry']['coordinates'][0]
        if 'name' in rail['properties']:
            temp['name'] = rail['properties']['name']
        if 'route_ref' in rail['properties']:
            temp['route'] = rail['properties']['route_ref']
        output_bus.append(temp)

    rail_array2 = []
    ra_crime = mongo.db.bus_stop.find()

    for rail in ra_crime:
        cnt = 0
        for cty in crime_types:
            if cty in rail['crimeCount'].keys():
                cnt = cnt + rail['crimeCount'][cty]
        rail_array2.append(cnt)
    st = pysal.esda.mapclassify.Natural_Breaks(rail_array2, k=3)

    data2 = np.array(rail_array)
    pp = pysal.User_Defined.make(bins=st.bins)(data2)
    ind = 0
    for i in output_bus:
        i['color'] = pp[ind] + 1
        ind = ind + 1

    ############################# POLICE #########################


    rails = mongo.db.police.find(query)
    output_police = []
    rail_array = []
    for rail in rails:
        cnt = 0
        temp = {}
        for cty in crime_types:
            if cty in rail['crimeCount'].keys():
                cnt = cnt + rail['crimeCount'][cty]
        temp['count'] = cnt
        rail_array.append(cnt)
        temp['lat'] = rail['geometry']['coordinates'][1]
        temp['lng'] = rail['geometry']['coordinates'][0]
        if 'name' in rail['properties']:
            temp['name'] = rail['properties']['name']
        output_police.append(temp)

    rail_array2 = []
    ra_crime = mongo.db.police.find()

    for rail in ra_crime:
        cnt = 0
        for cty in crime_types:
            if cty in rail['crimeCount'].keys():
                cnt = cnt + rail['crimeCount'][cty]
        rail_array2.append(cnt)
    st = pysal.esda.mapclassify.Natural_Breaks(rail_array2, k=3)

    data2 = np.array(rail_array)
    pp = pysal.User_Defined.make(bins=st.bins)(data2)
    ind = 0
    for i in output_police:
        i['color'] = pp[ind] + 1
        ind = ind + 1

    #
    # ranks = range(1, 6)
    # unique = []
    # [unique.append(item) for item in crime_array if item not in unique]
    # print unique
    #
    # try:
    #     crime_colors = pd.qcut(unique, 5, labels=ranks)
    # except ValueError as v:
    #     crime_colors = [2] * len(output)
    # print crime_colors
    # #k = 0
    # for i in output:
    #     i['color'] = crime_colors[unique.index(i['count'])]

    return json.dumps(
        {'roads': output, 'hospitals': output_hosp, 'railways': output_rail, 'parking': output_park, 'bus': output_bus,
         'police': output_police})


@app.route('/getCrimeCountBarChartPLaces', methods=['POST'])
def getCrimeCountBarcChart():
    print "INSIDE Bar"
    data = request.get_data()
    obj = json.loads(data)

    place_Id = obj['placeId']
    p = mongo.db.places.find({'place_id': place_Id})
    placeId = p[0]['_id']

    day = obj['day']
    place = mongo.db.places
    data = place.find({'_id': ObjectId(placeId)},
                      {'name': 1, 'populartimes': 1, 'maxPopularityCount': 1, 'crimeCount': 1, 'maxCount': 1})
    for i in data:
        data = i
        print data['maxPopularityCount']

    popularTime = {}
    crimeData = {}
    for ctr in range(0, 18):
        popularTime[ctr + 6] = data['populartimes'][day][ctr] * 100 / data['maxPopularityCount']
    if day in data['crimeCount'].keys():
        for ctr in range(0, 18):
            if str(ctr) in data['crimeCount'][day].keys():
                crimeData[ctr + 6] = data['crimeCount'][day][str(ctr)] * 100 / data['maxCount']
            else:
                crimeData[ctr + 6] = 0
    else:
        crimeData = {}
        for ctr in range(0, 18):
            crimeData[ctr + 6] = 0
    print popularTime
    print crimeData
    json_data = json.dumps({"popularTime": popularTime,
                            "crimeData": crimeData})
    return json_data

@app.route('/gaugedata', methods=['POST'])
def gauge():
    print "Loading Box Plot ...."
    data = request.get_data()
    params = json.loads(data)
    ID = params['id']

    # print params
    restaurant = mongo.db.restaurantsNew.find({})

    rating_list = []
    police_list = []
    parking_list = []
    hospital_list = []
    crime_list = []
    inspection_list = []
    injury_list = []

    for i in restaurant:
        rating_list.append(i['info']['rating'])

    DIST = params['Dist']

    query0 = {
        'info.location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': ''
            }
        }
    }
    query0['info.location']['$near']['$geometry']['coordinates'] = params['coordinates']
    query0['info.location']['$near']['$maxDistance'] = DIST

    restaurant = mongo.db.restaurantsNew.find(query0)
    restaurant_ids = []
    for r in restaurant:
        restaurant_ids.append(r['_id'])

    result = []

    query1 = {
        'geometry': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': 5000
            }
        }
    }
    query2 = {
        'location': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': ''
                }, '$minDistance': 0, '$maxDistance': 5000
            }
        }
    }

    for restaurant in restaurant_ids:
        temp = {}
        queries.set_query2_params(restaurant)
        r = mongo.db.restaurantsNew.find(queries.query2)[0]
        restaurant_location = r['info']['location']['coordinates']

        query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
        pol_count = mongo.db.police.find(query1).count()
        police_list.append(pol_count)

        query1['geometry']['$near']['$geometry']['coordinates'] = restaurant_location
        park_count = mongo.db.parking.find(query1).count()
        parking_list.append(park_count)

        query2['location']['$near']['$geometry']['coordinates'] = restaurant_location
        h_count = mongo.db.hospital.find(query2).count()
        hospital_list.append(h_count)

        injury_list.append(r['injuries'])

        c_count = 0
        queries.set_query2_params(restaurant)
        crime_counts = r['crime']  # count json
        for count in crime_counts:
            if count['id'] in params['crime_types']:
                c_count += count['value']
        crime_list.append(c_count)
        scores = []

        for inspection in r['inspection_list']:
            if 'SCORE' in inspection.keys() and (str(inspection['SCORE'])).isdigit():
                scores.append(int(inspection['SCORE']))
                inspection_list.append(sum(scores) / float(len(scores)))
        temp['id'] = str(ObjectId(r['_id']))
        temp['name'] = r['name']
        temp['image'] = r['info']['image_url']
        result.append(temp)

    norm_rating_list = [100 * float(i) / max(rating_list) for i in rating_list]
    norm_police_list = [100 * float(i) / max(police_list) for i in police_list]
    norm_parking_list = [100 * float(i) / max(parking_list) for i in parking_list]
    norm_hospital_list = [100 * float(i) / max(hospital_list) for i in hospital_list]
    norm_crime_list = [100 * float(i) / max(crime_list) for i in crime_list]
    norm_inspection_list = [100 * float(i) / max(inspection_list) for i in inspection_list]
    norm_injury_list = [100 * float(i) / max(injury_list) for i in injury_list]

    norm_net_list = [0] * len(norm_crime_list)

    # TODO logic
    for i in range(len(norm_crime_list)):
        norm_net_list[i] = (norm_rating_list[i] - norm_crime_list[i] + norm_hospital_list[i] + norm_parking_list[i] +
                            norm_police_list[i]) - norm_injury_list[i] + norm_inspection_list[i] / 7
    k = 0
    for o in result:
        o['police'] = norm_police_list[k]
        o['hospital'] = norm_hospital_list[k]
        o['parking'] = norm_parking_list[k]
        o['crime'] = norm_crime_list[k]
        o['net'] = norm_net_list[k]
        o['rating'] = norm_rating_list[k]
        o['inspection_score'] = norm_inspection_list[k]
        o['injury'] = norm_injury_list[k]
        o['isApnaVala'] = 0
        k += 1

    print result

    for r in result:
        if r['id'] == ID:
            print r['id'] + "   " + ID
            r['isApnaVala'] = 1
    return json.dumps(result)


@app.route('/getPlaceCrimeTypes', methods=['POST'])
def get_place_crime_types():
    data = request.get_data()
    params = json.loads(data)

    place = mongo.db.places
    place_Id = params['placeId']
    p = mongo.db.places.find({'place_id': place_Id})
    ID = p[0]['_id']

    query = {'_id': ""}
    query['_id'] = ObjectId(ID)

    p = place.find(query)[0]
    p_location = [p['geometry']['location']['lng'], p['geometry']['location']['lat']]

    query2 = {
        "location":
            {
                "$near":
                    {
                        "$geometry":
                            {
                                "type": "Point",
                                "coordinates": ""
                            },
                        "$minDistance": 0,
                        "$maxDistance": 1000
                    }
            }
    }

    query2['location']['$near']['$geometry']['coordinates'] = p_location
    crimes = mongo.db.nyccrime.find(query2)

    output = {}
    for crime in crimes:
        if crime['OFNS_DESC'] in output:
            output[crime['OFNS_DESC']] += 1
        else:
            output[crime['OFNS_DESC']] = 1

    import operator
    k = 0
    result = {}
    for i in sorted(output.items(), key=operator.itemgetter(1), reverse=True):
        result[i[0]] = i[1]
        k += 1
        if k == 5:
            break

    print result
    print json.dumps(result)
    return json.dumps(result)


if __name__ == '__main__':
    app.run(threaded=True)
