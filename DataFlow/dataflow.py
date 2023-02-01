import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp import bigquery_tools
import json
import time
import datetime



"""
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
"""

#Get Data from Pub/Sub and parse them.
def parsePubSubMessages(message):
    #Mapping message from PubSub
    #DecodePubSub message in order to deal with
  message = message.data.decode('utf-8')

   #Convert string decoded in json format(element by element)
  row = json.loads(message)

  return row

#Se añade el tiempo en procesar cada elemento
class add_processing_time(beam.DoFn):
    def process(self, element, element1, element2):
        window_start = str(datetime.datetime.now())
        output_data = {'Matricula': element, 'Pulsación': element1, 'Tensión': element2, 'processingTime': window_start}
        output_json = json.dumps(output_data)
        yield output_json.encode('utf-8')

class matricula(beam.DoFn):
    def process(self, element):
        mat = element['matricula']
        yield mat

class pulsacion(beam.DoFn):
    def process(self, element1):
        pul = element1['pulsacion']
        yield pul

class tension(beam.DoFn):
    def process(self, element2):
        ten = element2['tension']
        yield ten

#Beam PipeLine
def dataFlow(table):
    #Load schema from BigQuery/schemas folder
    with open(f"folder/{table}.json") as file:
        schema = json.load(file)
    
    #Declare bigquery schema
    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(schema))
    

    #Creation of Pipeline and set the options.

    #save_main_session: indicates that the main session 
    #should be saved during pipeline serialization, 
    #which enables pickling objects that belong to the main session.+

    options = PipelineOptions(save_main_session=True, streaming=True)
    with beam.Pipeline(options=options) as p:
        
          data = (
            #Read messages from PubSub
            p | "Read messages from PubSub" >> beam.io.ReadFromPubSub(subscription=f"Meter el PubSub del repositorio", with_attributes=True)
            #Parse JSON messages with Map Function and adding Processing timestamp
              | "Parse JSON messages" >> beam.Map(parsePubSubMessages)
              | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table = f"Nuestro Dataset",
                    schema = schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND 
        )
        # Tratamiento de Datos.
        #Se crea una ventana de 1 minuto.
         (data 
              | "Get Matricula value" >> beam.ParDo(matricula())
              | "Get Pulsación value" >> beam.ParDo(pulsacion())
              | "Get Tesión value" >> beam.ParDo(tension())
              | "WindowByMinute" >> beam.WindowInto(window.FixedWindows(60))
              | "MeanByWindow" >> beam.CombineGlobally(MeanCombineFn()).without_defaults()
              | "Add Window ProcessingTime" >> beam.ParDo(add_processing_time())
              | "WriteToPubSub" >> beam.io.WriteToPubSub(topic="Añadir nuestro topico de pubsub", with_attributes=False)
         )  

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    dataFlow("TablaDockerfile")


#Crear Dockerfile, requierements