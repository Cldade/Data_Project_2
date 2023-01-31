import random
import string

def generateVolanteData(matricula, timestamp):
    matricula = matricula    
    pulsacion = random.randrange(60,100)
    tension = random.randrange(80,120)

    #Return values
    return {
        "Matricula": matricula,
        "TimeStamp": timestamp,
        "Pulsacion": pulsacion,
        "Tension_arterial": tension
    }