# Serverless_Data_Processing_GCP
# EDEM_Master_Data_Analytics

""" Data Stream Generator:
The generator will publish a new record simulating a new transaction in our site.""" 

#Import libraries
import json
import string
import time
import uuid
import random
import logging
import argparse
import google.auth
from faker import Faker
from datetime import datetime, timedelta
from google.cloud import pubsub_v1

from volante import generateVolanteData as g_vol_d
from centralita import generateCentralitaData as g_cen_d
from camara import generateCamaraData as g_cam_d

fake = Faker()

#Input arguments
parser = argparse.ArgumentParser(description=('Aixigo Contracts Dataflow pipeline.'))
parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
parser.add_argument(
                '--topic_name',
                required=True,
                help='PubSub topic name.')

args, opts = parser.parse_known_args()

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id, topic_name):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_name = topic_name

    def publishMessages(self, message):
        json_str = json.dumps(message)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
        publish_future = self.publisher.publish(topic_path, json_str.encode("utf-8"))
        logging.info("A New transaction has been registered. Id: %s", message['Matricula'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

#Date generator
def random_date(start):
    current = start
    return current + timedelta(minutes=random.randrange(60))            

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    matricula = str(random.randrange(1000, 9999)) \
        + ' ' \
        +  random.choice(string.ascii_letters).upper() \
        +  random.choice(string.ascii_letters).upper() \
        +  random.choice(string.ascii_letters).upper()
    startDate = datetime(2023, 1, 1,00,00)
    timestamp = str(random_date(startDate))
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message: dict = g_vol_d(matricula, timestamp)
            pubsub_class.publishMessages(message)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

    try:
        while True:
            message2: dict = g_cen_d(matricula, timestamp)
            pubsub_class.publishMessages(message2)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

    try:
        while True:
            message3: dict = g_cam_d(matricula, timestamp)
            pubsub_class.publishMessages(message3)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)
