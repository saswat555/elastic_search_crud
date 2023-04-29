from elasticsearch import Elasticsearch
from flask import Flask, jsonify, request

app = Flask(__name__)


es = Elasticsearch(hosts=['http://192.168.1.17:9200'])

index_settings = {
    "settings": {
        "number_of_replicas": 2
    },
    "mappings": {
        "properties": {
            "Employee_Name": {
                "type": "text"
            },
            "Phone": {
                "type": "text"
            },
            "Start_Date": {
                "type":"date"
            },
            "End_Date": {
                "type":"date"
            },
            "Email": {
                "type": "text"
            },
            "Employee_Id": {
                "type" : "text"
            },
            "Team_Name" : {
                "type": "text"
            },
            "Role": {
                "type": "text"
            }
        }
    }
}

es.indices.create(index='users', body=index_settings)

INDEX_NAME = 'users'
@app.route('/users', methods=['POST'])
def create_user():
    data = request.json
    if not data or not data.get('Employee_Id') or not data.get('Phone'):
        return jsonify({'error': 'Missing fields'}), 400
    document = {
        'Employee_Name': data['Employee_Name'],
        'Phone': data['Phone'],
        'Start_Date' : data['Start_Date'],
        "End_Date" : data['End_Date'],
        "Employee_Id": data['Employee_Id'],
        "Team_Name": data['Team_Name'],
        "Role":data['Role'],
        "Phone":data["Phone"]
    }
    result = es.index(index=INDEX_NAME, body=document)
    return jsonify({'id': result['_id']}), 201

# Read a document
@app.route('/users/<id>', methods=['GET'])
def get_user(id):
    result = es.get(index=INDEX_NAME, id=id)
    return jsonify(result['_source']), 200

# Update a document
@app.route('/users/<id>', methods=['PUT'])
def update_user(id):
    data = request.json
    if not data or not data.get('Employee_Id') or not data.get('Phone'):
        return jsonify({'error': 'Missing fields'}), 400
    document = {
         'Employee_Name': data['Employee_Name'],
        'Phone': data['Phone'],
        'Start_Date' : data['Start_Date'],
        "End_Date" : data['End_Date'],
        "Employee_Id": data['Employee_Id'],
        "Team_Name": data['Team_Name'],
        "Role":data['Role'],
        "Phone":data["Phone"]
    }
    result = es.update(index=INDEX_NAME, id=id, body={'doc': document})
    return jsonify({'id': result['_id']}), 200

# Delete a document
@app.route('/users/<id>', methods=['DELETE'])
def delete_user(id):
    result = es.delete(index=INDEX_NAME, id=id)
    return jsonify({'id': result['_id']}), 200

# Search for documents
@app.route('/users', methods=['GET'])
def search_users():
    query = request.args.get('q')
    if query:
        body = {
            'query': {
                'match': {
                    '_all': query
                }
            }
        }
        result = es.search(index=INDEX_NAME, body=body)
        return jsonify([hit['_source'] for hit in result['hits']['hits']]), 200
    else:
        result = es.search(index=INDEX_NAME, body={'query': {'match_all': {}}})
        return jsonify([hit['_source'] for hit in result['hits']['hits']]), 200

# Run the Flask app
if __name__ == '__main__':
    app.run()