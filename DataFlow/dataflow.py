
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
        mat = element['Matricula']
        yield mat

class pulsacion(beam.DoFn):
    def process(self, element):
        pul = element['Pulsacion']
        yield pul

class tension(beam.DoFn):
    def process(self, element):
        ten = element['Tension']
        yield ten

class inclinacion(beam.DoFn):
    def process(self, element):
        inc = element['Inclinacion_cabeza']
        yield inc

class parpadeo(beam.DoFn):
    def process(self, element):
        par = element['Parpadeo']
        yield par

class tiempo(beam.DoFn):
    def process(self, element):
        tie = element['Tiempo_conduccion']
        yield tie

class cambios_velocidad(beam.DoFn):
    def process(self, element):
        cam = element['Cambio_velocidad']
        yield cam

class correciones_volante(beam.DoFn):
    def process(self, element):
        cor = element['Correcciones_Volante']
        yield cor

class alarma(beam.DoFn):
    def process(self,element):
        if  parpadeo() > 17 and inclinacion() > 15 and tiempo() > 150 and pulsacion() <= 65 and tension() < 80 and cambios_velocidad() == True and correciones_volante() == True:
            output_data = {'Matricula': matricula(), 'Pulsación': pulsacion(), 'Tensión': tension(), 'Inclinacion_cabeza': inclinacion(),
            'Parpadeo': parpadeo(), 'Tiempo': tiempo(), 'Cambios de velocidad': cambio_velocidad(), 'Correcciones al volante': correciones_volante()}
            output_json = json.dumps(output_data)
            yield output_json.encode('utf-8')




#Beam PipeLine
def dataFlow(table):
    #Load schema from BigQuery/schemas folder
    with open(f"./schemas/{table}.json") as file:
        schema = json.load(file)
    
    #Declare bigquery schema
    schema = bigquery_tools.parse_table_schema_from_json(json.dumps(schema))
    

    #Creation of Pipeline and set the options.

    #save_main_session: indicates that the main session 
    #should be saved during pipeline serialization, 
    #which enables pickling objects that belong to the main session.+

    options = PipelineOptions(project= 'dataproject2-376417',save_main_session=True, streaming=True)
    with beam.Pipeline(options=options) as p:
        
          data = (
            #Read messages from PubSub
            p | "Read messages from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/dataproject2-376417/subscriptions/vehiculo-sub", with_attributes=True)
            #Parse JSON messages with Map Function and adding Processing timestamp
              | "Parse JSON messages" >> beam.Map(parsePubSubMessages)
              | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table = f"dataproject2-376417.DataProject.vehiculo",
                    schema = schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND 
                )
        )

def dataFlowAlarm(table):
    #Load schema from BigQuery/schemas folder
    with open(f"schemas/{table}.json") as file:
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
            p | "Read messages from PubSub" >> beam.io.ReadFromPubSub(subscription=f"projects/dataproject2-376417/subscriptions/{table}", with_attributes=True)
            #Parse JSON messages with Map Function and adding Processing timestamp
              | "Parse JSON messages" >> beam.Map(parsePubSubMessages)
              | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table = f"dataproject2-376417:DataProject.{table}",
                    schema = schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND 
        )
        # Tratamiento de Datos.
        #Se crea una ventana de 1 minuto.
         (data 
              | "Get Alarmas" >> beam.WindowInto(alarma())
              | "WindowByMinute" >> beam.WindowInto(window.FixedWindows(60))
              | "MeanByWindow" >> beam.CombineGlobally(MeanCombineFn()).without_defaults()
              | "Add Window ProcessingTime" >> beam.ParDo(add_processing_time())
              | "WriteToPubSub" >> beam.io.WriteToPubSub(topic="projects/dataproject2-376417/topics/vehiculo", with_attributes=False)
         )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    dataFlow("bq_schema")
    #dataFlowAlarm("alarm_schema")


