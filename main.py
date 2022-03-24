from flask import Flask, request
from pymongo import MongoClient
from bson import json_util
from bson.objectid import ObjectId
import json
import config

app = Flask(__name__)

username = config.username
password = config.password
cluster = config.cluster
client = MongoClient(
    f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority")
db = client.sample_mflix

# print(db.command("serverStatus"))


@app.route("/get")
def getall():
    cursor = db.movies.find(limit=10)
    return {"message": "hello", "data": json.loads(json_util.dumps(cursor))}


@app.route("/get/<id>")
def get(id):
    cursor = db.movies.find({"_id": ObjectId(id)})
    return {"message": "hello", "data": json.loads(json_util.dumps(cursor))}


@app.route("/search/auto")
def searchAutoComplete():
    key = request.args.get('search')
    cursor = db.movies.aggregate([
        {
            '$search': {
                'autocomplete': {
                    'path': 'plot',
                    'query': key,
                    'tokenOrder': 'any',
                    'fuzzy': {
                        'maxEdits': 1,
                        'prefixLength': 1,
                        'maxExpansions': 256
                    }
                },
                'highlight': {
                    'path': 'plot'
                }
            }
        }, {
            '$limit': 100
        }, {
            '$project': {
                '_id': 1,
                'title': 1,
                'plot': 1,
                'highlights': {
                    '$meta': 'searchHighlights'
                }
            }
        }
    ])
    return {"message": "hello", "data": json.loads(json_util.dumps(cursor))}


@app.route("/search/auto/many")
def searchAutoMany():
    key = request.args.get('search')
    cursor = db.movies.aggregate([
        {
            '$search': {
                'compound': {
                    'should': [
                        {
                            'autocomplete': {
                                'query': key,
                                'path': 'title',
                            },
                        },
                        {
                            'autocomplete': {
                                'query': key,
                                'path': 'plot',
                            },
                        },
                    ],
                },
            }
        }, {
            '$limit': 100
        }, {
            '$project': {
                '_id': 1,
                'title': 1,
                'plot': 1,
                'highlights': {
                    '$meta': 'searchHighlights'
                }
            }
        }
    ])
    return {"message": "hello", "data": json.loads(json_util.dumps(cursor))}


@app.route("/search/regex")
def searchRegex():
    key = request.args.get('search')
    cursor = db.movies.aggregate([
        {
            '$search': {
                'regex': {
                    'path': 'fullplot',
                    'query': f'(.*){key}(.*)'
                }
            }
        }, {
            '$limit': 5
        }, {
            '$project': {
                '_id': 0,
                'fullplot': 1,
                'title': 1
            }
        }
    ])
    return {"message": "hello", "data": json.loads(json_util.dumps(cursor))}


@app.route("/search/auto/combined")
def searchAutoCombined():
    key = request.args.get('search')
    cursor = db.movies.aggregate([
        {
            '$search': {
                'compound': {
                    'should': [
                        {
                            'autocomplete': {
                                'query': key,
                                'path': 'title',
                            },
                        },
                        {
                            'autocomplete': {
                                'query': key,
                                'path': 'plot',
                            },
                        },
                        {
                            'regex': {
                                'path': 'fullplot',
                                'query': f'(.*){key}(.*)'
                            },
                        },
                    ],
                },
                "highlight": {
                    "path": ["title", "plot", "fullplot"]
                }
            }
        }, {
            '$limit': 100
        }, {
            '$project': {
                '_id': 1,
                'title': 1,
                'plot': 1,
                'fullplot': 1,
                'highlights': {
                    '$meta': 'searchHighlights'
                }
            }
        }
    ])
    return {"message": "hello", "data": json.loads(json_util.dumps(cursor))}
