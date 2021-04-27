import os
from elasticsearch import Elasticsearch
from datetime import date
import traceback
from email.parser import Parser

INDEX_NAME = 'enron-emails'
DATA_DIR = '../maildir0'
es = Elasticsearch()
p = Parser()


def build_index():
    # checks if index already exists and deletes if it that's the case to rebuild
    if es.indices.exists(INDEX_NAME):
        es.indices.delete(INDEX_NAME)
    res = es.indices.create(index=INDEX_NAME, ignore=400)
    put_mapping()
    return


def put_mapping():
    es.indices.put_mapping(
        index=INDEX_NAME,
        doc_type='enron-email',
        include_type_name=True,

        body=
        {

            "properties":
                {
                    "message_body": {"type": "text"},
                    "from": {"type": "text"},
                    "x-from": {"type": "text"},
                    "to": {"type": "text"},
                    "x-to": {"type": "text"},
                    "cc": {"type": "text"},
                    "x-cc": {"type": "text"},
                    "subject": {"type": "text"},
                    "content_size_in_bytes": {"type": "long", "store": "true"}

                }

        }
    )


def parse_eml(eml_source):
    data_source = open(eml_source)
    data = ""
    try:
        for line in data_source:
            data += line
    except Exception as err:
        data_source.close()
    finally:
        data_source.close()
    return data


def index_into_es_mapping(open_filename, subdirectory, filename, text):
    mail = p.parsestr(text)
    doc_mapper = {}

    headers = dict(mail._headers)
    for key, value in headers.items():
        key = key.lower()
        if not value.find(",") == -1 and key != "date" and key != "subject":
            value = value.split(",")
            doc_mapper[key] = value
        else:
            if key == "date" and value == "":
                value = date.today()
            doc_mapper[key] = value
    pass

    doc_mapper["sub_holder"] = subdirectory
    doc_mapper["file_name"] = filename
    doc_mapper["loaded_on"] = date.today()
    doc_mapper["message_body"] = mail._payload
    file_size = os.path.getsize(open_filename)
    doc_mapper["content_size_in_bytes"] = file_size
    try:
        es.index(index=INDEX_NAME, doc_type="enron-email", body=doc_mapper)
    except Exception as ex:
        traceback.print_exc()
        print("Unable to index the document {}".format(doc_mapper))
    return


if __name__ == "__main__":

    if not os.path.isdir(DATA_DIR):
        raise Exception("Enron corpus not available at : %s" % DATA_DIR)

    build_index()
    prefix_size = len(DATA_DIR) + 1

    for root, dirs, files in os.walk(DATA_DIR, topdown=False):
        directory = root[prefix_size:]
        print(directory)

        # Owner of the subset
        parts = directory.split('/', 1)
        subset_owner = parts[0]

        # parsing sub-directories
        if 2 == len(parts):
            sub_dir = parts[1]
        else:
            sub_dir = ''

        # Iterate over the files
        for filename in files:
            # load the contents of the file
            file_path = "{0}/{1}".format(root, filename)

            # Avoid any hidden system related folders
            if 'DS_Store' in file_path:
                continue

            contents = parse_eml(file_path)
            index_into_es_mapping(file_path, sub_dir, filename, contents)
