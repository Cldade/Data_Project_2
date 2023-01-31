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
from datetime import datetime
from google.cloud import pubsub_v1

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
        #logging.info("A New transaction has been registered. Id: %s", message['Transaction_Id'])

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")
            

#Generator Code
def generateMockData():
    matricula = str(random.randrange(1000, 9999)) \
        + ' ' \
        +  random.choice(string.ascii_letters).upper() \
        +  random.choice(string.ascii_letters).upper() \
        +  random.choice(string.ascii_letters).upper()
    
    pulsacion = random.randrange(60,100)
    tension = random.randrange(80,120)

    #Return values
    return {
        "Matricula": matricula,
        "Pulsacion": pulsacion,
        "Tension_arterial": tension,
    }

def run_generator(project_id, topic_name):
    pubsub_class = PubSubMessages(project_id, topic_name)
    #Publish message into the queue every 5 seconds
    try:
        while True:
            message: dict = generateMockData()
            pubsub_class.publishMessages(message)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator(args.project_id, args.topic_name)
