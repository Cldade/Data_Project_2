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
