FROM python:3.8.16-slim



COPY key.json /app/key.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/key.json

WORKDIR /app
COPY main.py main.py
COPY generador.py generador.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

CMD [ "python", "-u", "main.py" ]