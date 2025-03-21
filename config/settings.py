# config/settings.py
import os
from pathlib import Path
from dotenv import load_dotenv

def load_env():
    """Load environment variables from .env file"""
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)

# Database connection settings
def get_db_config():
    """Get database connection configuration from environment variables"""
    return {
        'ssh_host': os.getenv('SSH_HOST', 'ssh.pythonanywhere.com'),
        'ssh_username': os.getenv('SSH_USERNAME'),
        'ssh_password': os.getenv('SSH_PASSWORD'),
        'postgres_hostname': os.getenv('POSTGRES_HOSTNAME'),
        'postgres_port': int(os.getenv('POSTGRES_PORT', 0)),
        'db_name': os.getenv('DB_NAME'),
        'db_user': os.getenv('DB_USER'),
        'db_password': os.getenv('DB_PASSWORD'),
    }

# File processing settings
FILE_DELIMITER = '|'
ENCODING = 'utf-8'
BATCH_SIZE = 1000  # Number of records to process in a single database transaction

# Column mappings
COLUMNS = [
    'nombre_ejecutora', 'diresa', 'categoria', 'codpre', 'reportante',
    'tipsum', 'codmed', 'nombre_prod', 'tipo_prod', 'stk',
    'cpma', 'consumo_acum_4m', 'med', 'fechareporte', 'estado',
    'institucion', 'tipo_reportante', 'consumo_ult_mes', 'stk_ult_mes', 'indicador',
    'cpma_hace_12_meses_a', 'cpma_hace_24_meses_a', 'cpma_hace_36_meses_a', 'consumo_acum_12m', 'fin'
]