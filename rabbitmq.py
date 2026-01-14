import pika
import json
from config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, QUEUE_NAME

def publish_message(message: dict):
    try:
        print(f"Attempting to publish message to RabbitMQ: {message}")
        
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
        )
        
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        print(f"Message published to queue {QUEUE_NAME}")
        connection.close()
        
    except Exception as e:
        print(f"Error publishing message to RabbitMQ: {e}")
        raise