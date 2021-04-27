from elasticsearch import Elasticsearch
import json
from pprint import pprint

# from elasticsearch_dsl import Search, Q

INDEX_NAME = 'enron-emails'
NAME_TO_EMAIL_MAPPER = '../names_to_email.json'


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Connected')
    else:
        print('Cant connect!')
    return _es


def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    # pprint(res)


def query_expanded_search(query, es):
    with open(NAME_TO_EMAIL_MAPPER) as json_file:
        name_to_email = json.load(json_file)

    tokens = query.split()
    tokens = [i.lower() for i in tokens]

    all_hits = []

    for token in tokens:
        if token in name_to_email:
            email = name_to_email[token]
            email = email[0]
            body = {
                'query': {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "from": email
                                }
                            }
                        ],
                        "should": [
                            {
                                "match": {
                                    "subject": query
                                }
                            },
                            {
                                "match": {
                                    "message_body": query
                                }
                            }
                        ]
                    }
                }
            }

            res = es.search(index=INDEX_NAME, body=body)
            from_hits = res['hits']['hits']
            all_hits.extend(from_hits)

            body = {
                'query': {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "to": email
                                }
                            }
                        ],
                        "should": [
                            {
                                "match": {
                                    "subject": query
                                }
                            },
                            {
                                "match": {
                                    "message_body": query
                                }
                            }
                        ]
                    }
                }
            }

            res = es.search(index=INDEX_NAME, body=body)
            to_hits = res['hits']['hits']
            all_hits.extend(to_hits)

    res_sorted = []
    subjects = {}

    for num, doc in enumerate(all_hits):
        if doc['_source']['subject'] not in subjects:
            res_sorted.append(doc)
            subjects[doc['_source']['subject']] = True
    return res_sorted


def phrase_search(query):
    es = connect_elasticsearch()

    expanded_hits = query_expanded_search(query, es)

    # body = {
    #     "from": 5,
    #     "size": 400,
    #     'query': {
    #         'query_string': {
    #             'query': query,
    #             "fields": ["message_body"]
    #         }
    #     }
    # }

    body = {
        'query': {
            'query_string': {
                "query": query,
                "fields": ["subject", "message_body"]
            }
        }
    }

    # body = {
    #     "from": 5,
    #     "size": 30,
    #     'query': {
    #         'multi_match': {
    #             "query": query,
    #             "type": "most_fields",
    #             "fields": ["subject^3", 'from^2', 'to^2', "message_body"]
    #         }
    #     }
    # }

    # body = {
    #     "from": 5,
    #     "size": 30,
    #     'query': {
    #         'multi_match': {
    #             "query": query,
    #             "fields": ["subject", "message_body"]
    #         }
    #     }
    # }

    # body = {
    #     "query": {
    #         "multi_match": {
    #             "query": query,
    #             "type": "most_fields",
    #             "fields": ["subject", "message_body", "to", "from"]
    #         }
    #     }
    # }
    res = es.search(index=INDEX_NAME, body=body)

    matches = []
    subjects = {}

    if len(expanded_hits) > 0:
        count = 0
        for hit in expanded_hits:
            if count >= 3:
                break
            matches.append(hit['_source'])
            subjects[hit['_source']['subject']] = True
            count += 1

    all_hits = res['hits']['hits']
    # print(len(all_hits))
    # print('***')
    # print(res['hits'])

    all_hits.extend(expanded_hits)
    # print(len(all_hits))

    all_hits_sorted = sorted(all_hits, key=lambda k: k['_score'], reverse=True)
    # print(all_hits_sorted)

    for num, doc in enumerate(all_hits):
        if doc['_source']['subject'] not in subjects:
            matches.append(doc['_source'])
            subjects[doc['_source']['subject']] = True

    # prev = ''
    # for num, doc in enumerate(all_hits):
    #     if doc['_source']['subject'] != prev:
    #         matches.append(doc['_source'])
    #         prev = doc['_source']['subject']
    #     print("DOC ID:", doc["_id"], "--->", doc, type(doc), "\n")
    #
    #     # Use 'iteritems()` instead of 'items()' if using Python 2
    #     for key, value in doc.items():
    #         print(key, "-->", value)
    #
    #     # print a few spaces between each doc for readability
    #     print("\n\n")

    if len(matches) > 15:
        matches = matches[:15]

    print(len(matches))
    return matches


def boolean_search(query):
    return


def synonym_search(query):
    dictionary = PyDictionary()
    print(dictionary.synonym("good"))


# es = Elasticsearch([{'host': 'localhost'}, {'port': 9200}])
#
# count = Search(using=es).index("enron-email").count()
# print(count)
#
# s = Search(using=es).index("enron-email").query("match_all")  # .query("match", message_body="test")
#
# s.aggs.bucket('from_tags', 'terms', field='from')
# s.aggs.bucket('to_tags', 'terms', field='to')
#
# response = s.execute()
#
# print(response)
#
# print("\n=========== Top 10 from mails  ===========\n")
# for b in response.aggregations.from_tags.buckets:
#     print(b["key"] + " , " + str(b["doc_count"]))
# print("=============================================")
#
# print("\n=========== Top 10 to mails ===========\n")
# for b in response.aggregations.to_tags.buckets:
#     print(b["key"] + " , " + str(b["doc_count"]))
# print("=============================================")

if __name__ == '__main__':
    # phrase_search('phillip Risk Management Simulation')
    phrase_search('increased')
    # es = connect_elasticsearch()
    # if es is not None:
    #     # search_object = {'query': {'match': {'calories': '102'}}}
    #     # search_object = {'_source': ['title'], 'query': {'match': {'calories': '102'}}}
    #     # search_object = {'_source': ['title'], 'query': {'range': {'calories': {'gte': 20}}}}
    #
    #     search_object = {'query': {'match': {'message_body': 'available'}}}
    #     search(es, INDEX_NAME, json.dumps(search_object))

    # x = [{'a': 1, 'b': 3}, {'a': 5, 'b': 2}]
    # newlist = sorted(x, key=lambda k: k['a'], reverse=True)
    # print(newlist)
