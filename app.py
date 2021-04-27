from flask import Flask, request, jsonify
from flask_cors import CORS
from search import phrase_search
import json

app = Flask(__name__)
CORS(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/", methods=['POST'])
def search_handler():
    print("in search handle")
    req = request.json
    print(req)

    # with open("req_sample.json", "w") as outfile:
    #     json.dump(req, outfile)
    #
    # if req["req"] == "create_ppt" and req["source"].lower() == "wikipedia":
    #     result = create_ppt(req["title"])
    #     if result[0]:
    #         response = {
    #             "message": "Request successful",
    #             "title": result[1]
    #         }
    #         retu
    response = None
    try:
        matches = phrase_search(req)
        response = jsonify({'results': matches}), 200
    except Exception as err:
        response = jsonify({'results': []}), 400
    # response = jsonify({'msg': "a", "title": "b"})
    # response.headers.add("Access-Control-Allow-Origin", "*")
    return response


if __name__ == '__main__':
    app.run(debug=True, threaded=True, port=5000)
