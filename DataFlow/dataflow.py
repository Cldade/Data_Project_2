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

#DoFn 01: Select records with alarms
class alarma(beam.DoFn):
    def process(self,element):
        if  element['Parpadeo'] > 17 or element['Inclinacion_cabeza'] > 15 or element['Tiempo_conduccion'] > 150 or element['Pulsacion'] <= 65 or element['Tension_arterial'] < 80 or element['Cambios_velocidad'] == True or element['Correcciones_volante'] == True: 
            yield element

# DoFn 02: Get Matricula field from the input element
class getMatriculaDoFn(beam.DoFn):
    def process(self, element):
        yield element

# DoFn 03: Catching license plates with more than three alarms in 5 minutes
class topmatriculas(beam.DoFn):
    def process(self, element):
        if(element[0] >= 3):
            yield element

#Top alarm format
def estructura_top_alarmas(numero, matricula):
    yield {"Numero": numero, "Matricula": matricula}

#PTransform Classes
class WriteBigQuery(beam.PTransform):
    def expand(self, pcoll):
        with open(f"./schemas/bq_schema.json") as file:
            schema = json.load(file)

        (pcoll
            |"Write to BigQuery" >> beam.io.WriteToBigQuery(
                    table = f"dataproject2-376417.DataProject.{table}",
                    schema = schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND 
                )
        )

# DoFn 04 : Output data formatting
class OutputFormatDoFn(beam.DoFn):
    #Add process function
    def process(self, element):
        #Send a notification with the alarm
        output_msg = {"ProcessingTime": str(element['TimeStamp']), "message": f"{element['Matricula']} tiene una alarma por somnolencia."}
        #Convert the json to the proper pubsub format
        output_json = json.dumps(output_msg)
        yield output_json.encode('utf-8')

# DoFn 06 : Output data formatting alarm
class OutputFormatAlarmasDoFn(beam.DoFn):
    #Add process function
    def process(self, element):
        #Send a notification with the alarm
        output_msg = {"ProcessingTime": str(element[0]), "message": f"{element[1]} tiene una alarma por somnolencia."}
        #Convert the json to the proper pubsub format
        output_json = json.dumps(output_msg)
        yield output_json.encode('utf-8')

#Beam PipeLine
def dataFlow():
    global table
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
        table = "vehicle"
        (
            data
                | "Bigquery-vehiculos" >> WriteBigQuery()
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
        table = "alarm"
        (
            alarmas
                | "Bigquery-alarmas" >> WriteBigQuery()
        )
        
        top_alarmas = (
            data 
                | "Get MAtricula" >> beam.ParDo(getMatriculaDoFn())
                # Add Windows
                | "Set fixed window" >> beam.WindowInto(window.FixedWindows(20))
                | "Pair keys" >> beam.Map(lambda x: (x,1))
                # CombinePerKey
                | "CombinePerKey" >> beam.CombinePerKey(sum)
                # Swap the key,value to make the process easier
                | "Swap k,v" >> beam.KvSwap()
                | "Get top x matriculas" >> beam.ParDo(topmatriculas())
                # Define output format
                | "OutputFormat Alarmas_top" >> beam.ParDo(OutputFormatAlarmasDoFn())
                #Write alarm to PubSub Topic
                | "Send Push Notification Alarmas_top" >> beam.io.WriteToPubSub(topic=f"projects/dataproject2-376417/topics/alarmas_top", with_attributes=False)
        )
        

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    dataFlow()
    #dataFlowAlarm("alarm_schema")


