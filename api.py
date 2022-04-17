from flask import Flask, jsonify, url_for, redirect, request
from flask_pymongo import PyMongo
from flask_restful import Resource, Api
from flask_mongoengine import MongoEngine
import json
import random

app = Flask(__name__)
api = Api(app)

app.config["MONGO_DBNAME"] = "iot_db"
mongo = PyMongo(app, config_prefix='MONGO')
APP_URL = "http://127.0.0.1:5000"


class Iot(Resource):
    def get(self, id=None, range=None, emotion=None):
        data = []


        if range:
            # YYYYMMDDHHMMSS
            range = range.split('to')
            cursor = mongo.db.iot.find({"timestamp": {"$gt": int(range[0])}, "timestamp": {"$lt": int(range[1])}},
                                          {"_id": 0}).limit(10)
            for iot in cursor:
                iot['url'] = APP_URL + url_for('iots') + "/" + iot.get('id')
                data.append(iot)

            return jsonify({"range": range, "response": data})

        else:
            cursor = mongo.db.iot.find({}, {"_id": 0, "update_time": 0}).limit(10)

            for tweet in cursor:
                print(tweet)
                tweet['url'] = APP_URL + url_for('iots') + "/" + tweet.get('id')
                data.append(tweet)

            return jsonify({"response": data})


class Index(Resource):
    def get(self):
        return redirect(url_for("iot"))


api.add_resource(Index, "/", endpoint="index")
api.add_resource(Iot, "/api", endpoint="iot")
api.add_resource(Iot, "/api/range/<string:range>", endpoint="range")


@app.route('/get', methods=['GET'])
def get_data():
    return json.dumps({})


@app.route('/get/<years>', methods=['GET'])
def get_data(years):
    return json.dumps({})


if __name__ == '__main__':
    app.run(debug=True)
