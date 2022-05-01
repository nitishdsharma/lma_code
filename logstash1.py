import requests
import sys
import json
import argparse
from datetime import datetime
import configparser

logstashurl = "http://localhost:9200/_logstash/pipeline/"
# logstashurl = "http://localhost:9200/api/logstash/pipeline/"

headers = {'Content-Type': 'application/json'}
auth = ('<username>', '<password>')

def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

def list_logstash_pipelines():
    url = logstashurl + "?pretty"
    response = requests.get(url)
    pipelines_dict = json.loads(response.content.decode("utf8"))
    pipelines_list = [*pipelines_dict]
    return pipelines_list


def create_logstash_pipelines(pipeline_name, json_data):
    print("Creating a new pipeline: {}".format(pipeline_name))
    print("------------------------------------------")
    url = logstashurl + "{}?pretty".format(pipeline_name)
    data = json_data
    try:
        response = requests.put(url, headers=headers, data=json.dumps(data), auth=auth)
    except Exception as e:
        print(e)
    if response.status_code == 201:
        print("Pipeline {} creation is successful".format(pipeline_name))
    else:
        print("Issue in creating the {} pipeline".format(pipeline_name))
    print("------------------------------------------")


def update_logstash_pipelines(pipeline_name, json_data):
    print("Updating the {} pipeline with new information".format(pipeline_name))
    print("------------------------------------------")
    url = logstashurl + "{}?pretty".format(pipeline_name)
    data = json_data
    try:
        response = requests.put(url, headers=headers, data=json.dumps(data), auth=auth)
    except Exception as e:
        print(e)
    if response.status_code == 200:
        print("The pipeline {} has been updated successfully".format(pipeline_name))
    else:
        print("Some issue with pipeline update")
    print("------------------------------------------")


if __name__ == "__main__":
    now = datetime.now()
    now = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    if len(sys.argv) < 1:
        print("Issue in providing the commandline params")
        exit(0)
    else:
        piplines_list = list_logstash_pipelines()
        parser = argparse.ArgumentParser(description=''' Note: Specify the appropriate parameters in the commandline. ''' )
        # parser.add_argument("--pipelinename", nargs='?', const='', type=str, help="Specify the pipeline name to be created or updated")
        parser.add_argument("--masterconfig", nargs='?', const='', type=str, help="Specify the master config file name")
        # parser.add_argument("--pipelineconfig", nargs='?', const='', type=str, help="Specify the pipeline config file name")
        args = parser.parse_args()

        masterconfig = args.masterconfig

        config = configparser.ConfigParser()
        config.read("{}".format(masterconfig))

        pipeline_name = config.get('default','pipelinename')
        pipelinefile = config.get('default','pipelineconfigfilename')
        description = config.get('default', 'description')
        username = config.get('default', 'username')

        fp = open("{}".format(pipelinefile), "r")
        pipeline_config_data = fp.read()
        fp.close()

        json_data = {
            "description": description,
            "last_modified": "{}".format(now),
            "pipeline_metadata": {
                "type": "logstash_pipeline",
                "version": "1"
            },
            "username": username,
            "pipeline": pipeline_config_data,
            "pipeline_settings": {
            "pipeline.workers": 1,
            "pipeline.batch.size": 125,
            "pipeline.batch.delay": 50,
            "queue.type": "memory",
            "queue.max_bytes.number": 1,
            "queue.max_bytes.units": "gb",
            "queue.checkpoint.writes": 1024
            }
        }

        if pipeline_name not in piplines_list:
            create_logstash_pipelines(pipeline_name, json_data)
        elif pipeline_name in piplines_list:
            existing_pipeline_response = requests.get(logstashurl + "{}?pretty".format(pipeline_name), auth=auth)
            e_json_data = existing_pipeline_response.content
            e_json_data = e_json_data.decode("utf8")
            e_json_data = json.loads(e_json_data)['{}'.format(pipeline_name)]
            if (ordered(json_data["pipeline"]) == ordered(e_json_data["pipeline"])):
                print("Pipeline content is same, no need to update the pipeline")
            else:
                update_logstash_pipelines(pipeline_name, json_data)