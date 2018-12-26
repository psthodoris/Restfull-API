import json
from flask import Flask, request
from flask_restful import Api, Resource, reqparse
from pymongo import MongoClient
from bson import json_util

# Sissy Dalampira-Kiprigli 2744 odalampir@csd.auth.gr
# Papadopoulos Theodoros 2785 pstheodor@csd.auth.gr

# Using code from github.com/vasisouv
# ATTENTION : Change database name with yours' database

app = Flask(__name__)


if __name__ == "__main__":

    # Mongo Database Connection
    client = MongoClient("localhost", 27017, maxPoolSize=50)
    db = client['apis']
    collection = db['apis']
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

            # Database-deletion of hashtag
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

        # 25.12.2018 Second Implementation
        def post(self):
            print("Post function called.")
            # Parse arguments
            parser.add_argument('user', type=str, required=True, help='Error finding username.')
            parser.add_argument('message', type=str, required=True, help='Error finding message.')
            parser.add_argument('age', type=int, required=True, help='Error finding age.')
            args = parser.parse_args()
            user = args['user']
            message = args['message']
            age = args['age']
            # Setting up the result to show
            answer = {"user": user, "message": message, "age": age}
            ans = json_util.dumps(answer)
            result = json.loads(ans)
            # If a record already exists
            if collection.find_one({"user": user}) and collection.find_one({"message": message}) \
                    and collection.find_one({"age": age}):
                return {"response": "input already exists."}, 400
            else:
                # Database insert
                collection.insert(answer)
                return result, 201


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
                 data = json_util.dumps(data)
                 loaded_data = json.loads(data)
                 return loaded_data

            elif hashtag:
                 print("Get 2")
                 documents = collection.find({'entities.hashtags.text': hashtag})
                 for document in documents:
                     data.append(document)
                 data = json_util.dumps(data)
                 loaded_data = json.loads(data)
                 return loaded_data

            else:
                print("Get 3")
                documents = collection.find()
                for document in documents:
                    data.append(document)
                data = json_util.dumps(data)
                loaded_data = json.loads(data)
                return loaded_data


    # Initializing the end-points
    api = Api(app)
    api.add_resource(Getter, "/tweets", endpoint="tweets_endpoint")
    api.add_resource(Getter, "/tweets/hashtag/<string:hashtag>", endpoint="hashtag_endpoint")
    api.add_resource(Deleter, "/tweets/hashtag/<string:hashtag>", endpoint="delete_endpoint")
    api.add_resource(Poster, "/post", endpoint="post_endpoint")


app.run(debug=True, host='127.0.0.1', port=5110)
