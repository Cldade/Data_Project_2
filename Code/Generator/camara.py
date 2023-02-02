import random
import string

def generateCentralitaData(matricula, timestamp):
    matricula = matricula    
    parpadeo = random.randrange(12,21) #nº de parpadeos por minuto. De media son 15/min en estado relajado
    cabeceo = random.randrange(1,5) #nº de cabeceos por minuto. En estado relajado esto no pasa.
    #Return values
    return {
        "Matricula": matricula,
        "TimeStamp": timestamp,
        "Nº de parpadeos": parpadeo,
        "Nº de cabeceos": cabeceo
    }