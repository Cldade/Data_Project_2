
import argparse
import json
import logging
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import (PipelineOptions, StandardOptions)
from apache_beam.transforms.combiners import MeanCombineFn
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.transforms.core import CombineGlobally
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp import bigquery_tools
import datetime

#Get Data from Pub/Sub and parse them.
def parsePubSubMessages(message):
    #Decode PubSub message in order to deal with
    pubsubmessage = message.data.decode('utf-8')
    #Convert string decoded in json format(element by element)
    row = json.loads(pubsubmessage)
    #Logging
    logging.info("Receiving message from PubSub:%s", pubsubmessage)
    #Return function
    return row

#Se añade el tiempo en procesar cada elemento
class add_processing_time(beam.DoFn):
    def process(self, element, element1, element2):
        window_start = str(datetime.datetime.now())
        output_data = {'Matricula': element, 'Pulsación': element1, 'Tensión': element2, 'processingTime': window_start}
        output_json = json.dumps(output_data)
        yield output_json.encode('utf-8')

#Nos quedamso solo con los registros que sean alarmas
class alarma(beam.DoFn):
    def process(self,element):
        if  element['Parpadeo'] > 17: 
            yield element

# DoFn 05 : Output data formatting
class OutputFormatDoFn(beam.DoFn):
    #Add process function
    def process(self, element):
        #Send a notification with the alarm
        output_msg = {"ProcessingTime": str(element['TimeStamp']), "message": f"{element['Matricula']} tiene una alarma por somnolencia."}
        #Convert the json to the proper pubsub format
        output_json = json.dumps(output_msg)
        yield output_json.encode('utf-8')

#Beam PipeLine
def dataFlow():
    #Load schema from BigQuery/schemas folder
    with open(f"./schemas/bq_schema.json") as file:
        schema = json.load(file)
    
    #Declare bigquery schema
    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(schema))
    
    options = PipelineOptions(project= 'dataproject2-376417',save_main_session=True, streaming=True)
    with beam.Pipeline(options=options) as p:
        #Leermos los datos de los vehículos
        data = (
            #Read messages from PubSub
            p | "Read messages from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/dataproject2-376417/subscriptions/vehiculo-sub", with_attributes=True)
            #Parse JSON messages with Map Function and adding Processing timestamp
              | "Parse JSON messages" >> beam.Map(parsePubSubMessages)
              
        )
        #Guardamos todos los datos tal cual los leemos
        (
            data
                |"Write to BigQuery-Vehiculos" >> beam.io.WriteToBigQuery(
                    table = f"dataproject2-376417.DataProject.vehiculo",
                    schema = schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND 
                )
        )
        #Identificamos los vehículos que puedan tener alarmas
        alarmas = (
            data
                # Searching alarms
                | "Seleccionando Alarmas" >> beam.ParDo(alarma())
        )
        #Publicamos en el tópico las matrículas con alarmas
        (
            alarmas 
                # Define output format
                | "OutputFormat" >> beam.ParDo(OutputFormatDoFn())
                #Write alarm to PubSub Topic
                | "Send Push Notification" >> beam.io.WriteToPubSub(topic=f"projects/dataproject2-376417/topics/alarmas", with_attributes=False)
        )
        #Nos guardamos los datos de los vehículos con alarmas  
        (
            alarmas
                |"Write to BigQuery-Alarmas" >> beam.io.WriteToBigQuery(
                    table = f"dataproject2-376417.DataProject.alarma",
                    schema = schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND 
                )
        )
        

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    dataFlow()
    #dataFlowAlarm("alarm_schema")


