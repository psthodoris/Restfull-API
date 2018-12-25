from flask import Flask, jsonify,request
from flask_restful import Api, Resource, reqparse
from pymongo import MongoClient
from bson import json_util
import requests


# Sissy Dalampira-Kiprigli 2744 odalampir@csd.auth.gr
# Papadopoulos Theodoros 2785 pstheodor@csd.auth.gr

# Using code from github.com/vasisouv
# ATTENTION : Change database name with yours' database

app = Flask(__name__)


if __name__ == "__main__":

    # Mongo Database Connection
    client = MongoClient("localhost", 27017, maxPoolSize=50)
    db = client['tweets']
    collection = db['pspi students']
    # Parser for the get function
    parser = reqparse.RequestParser()

    # Class handles DELETE requests
    class Deleter(Resource):

        def delete(self,hashtag=None):
            print("Delete function called")

            # find the amount of all tweets
            counter0 = collection.count()
            print("All the tweets are : " + str(counter0))

            # count the tweets before a delete -action with current hashtag
            counter1 = collection.find({'entities.hashtags.text': hashtag})
            count1 = counter1.count()
            print("Number of tweets with the hashtag: " + hashtag + " are :" + str(count1))

            # Here code for database deletion of hashtag
            myquery = {'entities.hashtags.text': hashtag}
            # collection.delete_one(myquery)
            collection.delete_many(myquery)

            # The counter2 after the deletion should be zero
            counter2 = collection.find({'entities.hashtags.text': hashtag})
            count2 = counter2.count()
            print("After deletion the tweets with hashtag:" + hashtag + " are :" + str(count2))
            # Initializing a JSON format output
            answer = []
            answer = {"removedCount": count1}
            # returs JSON format with key 'removedCount' and value the removed amount of tweets
            # if count2 == 0:

            return answer, 200


    # Class handles POST requests
    class Poster(Resource):

        # A paradigm http://127.0.0.1:5110/post?{"user":"alex","message":"demo","age":"91"}
        def post(self):
            print("Post function called.")
            # 25.12.2018 implementation
            try:
                foofer = request.get_json(force=True)
            except:
                return "json input error", 400
            if not ('user' | 'message' | 'age') in foofer:
                return "missing parameters", 400
            answer = {
                'user': foofer['user'],
                'message': foofer['message'],
                'age': foofer['age']
            }
            collection.insert(answer)
            return str(answer), 201

        # def post2(self):
          #   print("Post function called.")
            # New Implementation
            # Source : https://github.com/chaitjo/flask-mongodb/blob/master/api.py
           # json_data = request.get_json(force=True)
            # json_data = requests.get(url).json()
          #  user = str(json_data['user'])
          #  message = str(json_data['message'])
          #  age = str(json_data['age'])
          #  answer = {'user': user, 'message': message, 'age': age}
            # return jsonify(u=user, p=message, ag=age)
          #  if collection.find_one({"user": user}):
          #      return {"response": "input already exists."}, 400
          #  else:
          #      collection.insert(answer)
          #      return str(answer), 201

    # Class handles GET requests
    class Getter(Resource):

        def get(self, hashtag=None):
            data = []
            if not(parser.add_argument('morethan', type=int, required=False, help='Error when parsing morethan')):
                return "unexpected morethan input", 404

            args = parser.parse_args()
            limit = args['morethan']
            if limit:
                 print("Get 1")
                 documents = collection.find({('entities.hashtags.' + str(limit)): {'$exists': True}})
                 for document in documents:
                     data.append(document)
                 return jsonify(json_util.dumps(data))

            elif hashtag:
                 print("Get 2")
                 documents = collection.find({'entities.hashtags.text': hashtag})
                 for document in documents:
                     data.append(document)
                 return jsonify(json_util.dumps(data))

            else:
                print("Get 3")
                documents = collection.find()
                for document in documents:
                    data.append(document)
                return jsonify(json_util.dumps(data))


    # Initializing the end-points
    api = Api(app)
    api.add_resource(Getter, "/tweets", endpoint="tweets_endpoint")
    # api.add_resource(Functionality, "/tweets?morethan=<int:more_than>", endpoint="tweets_more_than_endpoint")
    api.add_resource(Getter, "/tweets/hashtag/<string:hashtag>", endpoint="hashtag_endpoint")
    api.add_resource(Deleter, "/tweets/hashtag/<string:hashtag>", endpoint="delete_endpoint")
    api.add_resource(Poster, "/post", endpoint="post_endpoint")

app.run(debug=True, host='127.0.0.1', port=5110)
