# Data_Project_2
`gcloud auth application-default login`

# Multiple Cliente
Para poder ejecutar el cliente multiple hay que seguir los siguientes pasos:
1. Abrir una temrinal en la carpeta Code/Multiple_client/client
2. Ejecutar `Docker buil -t client:latest .`
3. Ejecutar el archivo main.py que hay en Code/Multiple_client `python3 main.py -t 100 -e 2 -i client`

IMPORTANTE: en la carpeta Code/Multiple_client/client hay que poner el archivo key.json con las credenciales de la cuenta de servicio de google. Este archivo no se puede subir a git, por eso esta explicitamente puesto en '.gitignore' que no se suba.