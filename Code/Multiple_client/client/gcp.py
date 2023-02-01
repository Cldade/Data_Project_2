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