import psycopg2
from app.config.settings import settings
import logging

def get_connection():
    try:
        return psycopg2.connect(
            host=settings["host"],
            user=settings["user"],
            password=settings["password"],
            database=settings["database"]
        )
    except Exception as e:
        logging.error(f"Erro ao conectar ao banco de dados: {e}")
        raise
