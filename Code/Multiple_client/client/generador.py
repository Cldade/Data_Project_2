#Libraries
from google.cloud import pubsub_v1
import logging
import json
from datetime import datetime, timedelta
import random
import string
import time

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

class vehicle_data():
    #Generamos los datos del vehículo
    def vehicle_data():
        return str(random.randrange(1000, 9999)) \
            + ' ' \
            +  random.choice(string.ascii_letters).upper() \
            +  random.choice(string.ascii_letters).upper() \
            +  random.choice(string.ascii_letters).upper()

#Generamos la fecha
def random_date(start):
    current = start
    return current + timedelta(seconds=2)    

startDate = datetime(2023, 1, 1,00,00)
fecha_ant = datetime(2023, 1, 1,00,00)
tiempo = -2
tiempo_ant = 0

#Generamos los datos del vehículo
def vehicle_data(fecha, tiempo, matricula):
    timestamp = fecha
#Datos del volante
    pulsacion = random.randrange(60,100)
    tension = random.randrange(80,120)
#Datos de la camara
    inclinacion = random.randrange(0,80)
    #Tiempo de parpadeo medio 100ms
    parpadeo = random.randrange(50,300)
#Datos de la centralita
    tiempo = tiempo
    cambios_velocidad = bool(random.getrandbits (1))
    correciones_volante = bool(random.getrandbits (1))
#Respuesta
    return {
        "Matricula": matricula,
        "TimeStamp": timestamp,
        "Pulsacion": pulsacion,
        "Tension_arterial": tension,
        "Inclinacion_cabeza": inclinacion,
        "Parpadeo": parpadeo,
        "Tiempo_conduccion": tiempo,
        "Cambios_velocidad": cambios_velocidad,
        "Correcciones_volante": correciones_volante
    }

#Publicador de mensajes
def run_generator(project_id):
    pubsub_class = PubSubMessages(project_id, "vehiculo")
    fecha = datetime(2023, 1, 1,00,00)
    tiempo = 0
    matricula = str(random.randrange(1000, 9999)) \
        + ' ' \
        +  random.choice(string.ascii_letters).upper() \
        +  random.choice(string.ascii_letters).upper() \
        +  random.choice(string.ascii_letters).upper()
    i = 0
    n = random.randrange(0,200)
    #Publish message into the queue every 5 seconds
    try:
        while i <= n:
            #Publicando datos del sensor `volante'
            message_volante: dict = vehicle_data(str(fecha), tiempo, matricula)
            pubsub_class.publishMessages(message_volante)
            #it will be generated a transaction each 2 seconds
            time.sleep(5)
            fecha = random_date(fecha)
            tiempo = tiempo + 2
    except Exception as err:
        logging.error("Error while inserting data into out PubSub Topic: %s", err)
    finally:
        pubsub_class.__exit__()
        #pubsub_class_centralita.__exit__()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_generator("dataproject2-376417")