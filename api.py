from bson import json_util
from flask import Flask, jsonify, url_for, redirect, request, Response
from flask_pymongo import PyMongo
from flask_restful import Resource, Api
from flask_mongoengine import MongoEngine
import json
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)
api = Api(app)
app.config["MONGO_DBNAME"] = "industry"
app.config["MONGO_URI"] = "mongodb://localhost:27017/industry"

mongo = PyMongo(app)
APP_URL = "http://127.0.0.1:5000"

def create_app():
    app = Flask(__name__)
    CORS(app)

    @app.route('/api', methods=['GET'])
    @cross_origin()
    def data_api():
        data = []
        iot = mongo.db.iot.find().limit(10)
        response = json_util.dumps(iot)
        return Response(response, mimetype='application/json')

    return app

# class Iot(Resource):
#     def get(self, id=None, range=None, emotion=None):
#         data = []
#
#         if range:
#             # YYYYMMDDHHMMSS
#             range = range.split('to')
#             cursor = mongo.db.iot.find({"timestamp": {"$gt": int(range[0])}, "timestamp": {"$lt": int(range[1])}},
#                                        {"_id": 0}).limit(10)
#             for iot in cursor:
#                 iot['url'] = APP_URL + url_for('iots') + "/" + iot.get('id')
#                 data.append(iot)
#
#             return jsonify({"range": range, "response": data})
#
#         else:
#             cursor = mongo.db.iot.find({}, {"_id": 0, "update_time": 0}).limit(10)
#
#             for tweet in cursor:
#                 print(tweet)
#                 tweet['url'] = APP_URL + url_for('iots') + "/" + tweet.get('id')
#                 data.append(tweet)
#
#             return jsonify({"response": data})
#
#
# class Index(Resource):
#     def get(self):
#         return redirect(url_for("iot"))
#
#
# api.add_resource(Index, "/", endpoint="index")
# api.add_resource(Iot, "/api", endpoint="iot")
# api.add_resource(Iot, "/api/range/<string:range>", endpoint="range")


@app.route('/get', methods=['GET'])
def get_data():
    data =[]
    iot = mongo.db.iot.find().limit(10)
    response = json_util.dumps(iot)
    return Response(response,mimetype='application/json')


@app.route('/get/<years>', methods=['GET'])
def get_data_years(years):
    return json.dumps({})


if __name__ == '__main__':
    app.run(host='localhost', port=5000, debug=True)
