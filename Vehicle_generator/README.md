# Vehicle_generator
Código generador de datos de vehículos.

Este código se encarga de simular un IoT sobre vehículos. Su finalidad es monitorizar al conductor para poder saber si se esta duermiendo o no.

## Cliente

Este generador se compone de un contenedor que llamaremos 'cliente' al crearlo, que tiene la lógica del vehículo y es el encargado de generar los datos ficticios y enviarlos a nuestro sistema de mensajería PubSub. El código se encuentra dentro de la carpeta **vehicle**. Para usarlo, se debe generar un contenedor cliente y darle el nombre de 'cliente'.

## Generador de Clientes

El script main.py de la carpeta Vehicle_generator simula la existencia de multiples vehículos enviando datos. Para ello generará aleatoriamente un numero de contenedores de tipo cliente que enviarán datos.

Para usar este script se debe ejecutar:

```
main.py -t <topcontainers> -e <elapsedtime> -i <imagename>
```

# Pasos para poder poner en marcha el generador de datos

Para poder ejecutar el cliente multiple hay que seguir los siguientes pasos:
1. Abrir una terminal en la carpeta Vehicle_generator/vehicle/
2. Ejecutar `Docker buil -t client:latest .`
3. Ejecutar el archivo main.py que hay en Vehicle_generator/ `python3 main.py -t 100 -e 2 -i client`

IMPORTANTE: en la carpeta Vehicle_generator/vehicle/ hay que poner el archivo key.json con las credenciales de la cuenta de servicio de google cloud. Este archivo no se puede subir a git, por eso esta explicitamente puesto en '.gitignore' que no se suba.