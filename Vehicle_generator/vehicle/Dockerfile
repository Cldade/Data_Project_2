FROM python:3.8.16-slim

WORKDIR /app

COPY key.json /app/key.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/key.json

COPY main.py main.py
COPY generador.py main.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

CMD [ "python", "-u", "main.py" ]