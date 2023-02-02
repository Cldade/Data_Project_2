import random
import string

def generateCentralitaData(matricula, timestamp):
    matricula = matricula    
    km = random.randrange(150,350) #km recorridos de manera continuada (sin apagar el motor)
    t_motor = random.randrange(90,180) #min consecutivos con el motor encendido

    #Return values
    return {
        "Matricula": matricula,
        "TimeStamp": timestamp,
        "km recorridos": km,
        "tiempo de conducción": t_motor
    }