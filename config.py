import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
QUEUE_NAME = 'hikvision_queue'

HIKVISION_BASE_URL = os.getenv('HIKVISION_BASE_URL', 'https://192.168.100.15')
HIKVISION_AK = os.getenv('HIKVISION_AK', 'your_api_key_here')
HIKVISION_SIGNATURE = os.getenv('HIKVISION_SIGNATURE', 'your_signature_here')