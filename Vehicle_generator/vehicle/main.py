import json
import os
import time
import logging
import generador as g
import os
from google.oauth2.service_account import Credentials

credentials = None

if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
    credentials = Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

if not credentials:
    print("Credenciales no encontradas")
else:
    print("Autenticación exitosa")


user_id=os.getenv('USER_ID')
topic_id=os.getenv('TOPIC_ID')
time_lapse=int(os.getenv('TIME_ID'))

def generatedata():
    data={}
    data["userid"]=user_id
    return json.dumps(data)

def senddata():
    g.logging.getLogger().setLevel(logging.INFO)
    g.run_generator("dataproject2-376417")



while True:
    senddata()
    time.sleep(time_lapse)
