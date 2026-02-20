import json
import threading
import pika
import logging
from datetime import datetime
from worker import send_to_hikvision, update_employee_kib, assign_privilege_groups
from config import (RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, 
                    QUEUE_CREATE_PERSON, QUEUE_UPDATE_KIB, QUEUE_ASSIGN_PRIVILEGE)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('worker_pubsub.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_connection():
    """Create RabbitMQ connection with proper settings"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection_params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(connection_params)

def check_rabbitmq_connection():
    """Check if RabbitMQ connection is working"""
    try:
        print(f"Checking RabbitMQ connection...")
        print(f"  Host: {RABBITMQ_HOST}")
        print(f"  Port: {RABBITMQ_PORT}")
        print(f"  User: {RABBITMQ_USER}")
        
        connection = create_connection()
        channel = connection.channel()
        
        # Test basic operation
        channel.queue_declare(queue='connection_test', passive=False)
        
        connection.close()
        
        print("✓ RabbitMQ connection successful!")
        return True
        
    except pika.exceptions.AMQPConnectionError as e:
        print(f"✗ RabbitMQ connection failed: {e}")
        print(f"  Make sure RabbitMQ is running and credentials are correct")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

def publish_to_queue(queue_name, message):
    """Publish message to specific queue"""
    connection = create_connection()
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
    )
    
    connection.close()

# Worker 1: Create Person
def callback_create_person(ch, method, properties, body):
    try:
        # Log raw message received
        logger.info(f"[CREATE PERSON] Received message: {body.decode('utf-8')}")
        
        message = json.loads(body)
        employee = message.get("employee")
        
        logger.info(f"[CREATE PERSON] Processing: {employee.get('name')} - Data: {json.dumps(employee, indent=2)}")
        print(f"\n[CREATE PERSON] Processing: {employee.get('name')}")
        result = send_to_hikvision(employee)
        
        if result:
            person_id = result.get("personId")
            kib_number = employee.get("kib_number")
            
            # Publish to UPDATE KIB queue
            if person_id and kib_number:
                publish_to_queue(QUEUE_UPDATE_KIB, {
                    "personId": person_id,
                    "kib_number": kib_number,
                    "employee": employee
                })
                print(f"[CREATE PERSON] ✓ Sent to UPDATE KIB queue: Person {person_id}")
            
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"[CREATE PERSON] ✓ Message processed successfully")
        
    except Exception as e:
        logger.error(f"[CREATE PERSON] Error: {e}", exc_info=True)
        print(f"[CREATE PERSON] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Worker 2: Update KIB
def callback_update_kib(ch, method, properties, body):
    try:
        # Log raw message received
        logger.info(f"[UPDATE KIB] Received message: {body.decode('utf-8')}")
        
        message = json.loads(body)
        person_id = message.get("personId")
        kib_number = message.get("kib_number")
        employee = message.get("employee")
        
        logger.info(f"[UPDATE KIB] Processing: Person {person_id}, KIB: {kib_number}")
        print(f"\n[UPDATE KIB] Processing: Person {person_id}")
        success = update_employee_kib(person_id, kib_number)
        
        if success:
            # Map regionals to privilege groups
            regional_mapping = {
                "zona-i": "1",
                "zona-ii": "2",
                "zona-iii": "6",
                "zona-iv": "7",
                "tuks": "8",
                "kawasan": "9"
            }
            
            privilege_groups = []
            regionals = employee.get("regionals", [])
            for regional in regionals:
                slug = regional.get("slug", "")
                if slug in regional_mapping:
                    privilege_groups.append(regional_mapping[slug])
            
            # Publish to ASSIGN PRIVILEGE queue
            if privilege_groups:
                publish_to_queue(QUEUE_ASSIGN_PRIVILEGE, {
                    "personId": person_id,
                    "privilege_groups": privilege_groups
                })
                print(f"[UPDATE KIB] ✓ Sent to ASSIGN PRIVILEGE queue: {len(privilege_groups)} groups")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"[UPDATE KIB] ✓ Message processed successfully")
        
    except Exception as e:
        logger.error(f"[UPDATE KIB] Error: {e}", exc_info=True)
        print(f"[UPDATE KIB] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Worker 3: Assign Privilege Groups
def callback_assign_privilege(ch, method, properties, body):
    try:
        # Log raw message received
        logger.info(f"[ASSIGN PRIVILEGE] Received message: {body.decode('utf-8')}")
        
        message = json.loads(body)
        person_id = message.get("personId")
        privilege_groups = message.get("privilege_groups")
        
        logger.info(f"[ASSIGN PRIVILEGE] Processing: Person {person_id}, Groups: {privilege_groups}")
        print(f"\n[ASSIGN PRIVILEGE] Processing: Person {person_id}")
        assign_privilege_groups(person_id, privilege_groups)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"[ASSIGN PRIVILEGE] ✓ Completed for person {person_id}")
        print(f"[ASSIGN PRIVILEGE] ✓ Completed for person {person_id}")
        
    except Exception as e:
        logger.error(f"[ASSIGN PRIVILEGE] Error: {e}", exc_info=True)
        print(f"[ASSIGN PRIVILEGE] Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_worker(queue_name, callback, worker_name):
    """Start a worker for specific queue"""
    print(f"[{worker_name}] Starting worker...")
    connection = create_connection()
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    print(f"[{worker_name}] Worker started. Waiting for messages on queue: {queue_name}...")
    channel.start_consuming()

def start_all_workers():
    """Start all 3 workers in separate threads"""
    # Check RabbitMQ connection first
    logger.info("="*60)
    logger.info("Starting Worker Pub/Sub System...")
    logger.info("="*60)
    
    if not check_rabbitmq_connection():
        logger.error("ERROR: Cannot connect to RabbitMQ!")
        print("\n" + "="*60)
        print("ERROR: Cannot connect to RabbitMQ!")
        print("Please check your .env configuration and ensure RabbitMQ is running")
        print("="*60 + "\n")
        return
    
    workers = [
        (QUEUE_CREATE_PERSON, callback_create_person, "CREATE PERSON"),
        (QUEUE_UPDATE_KIB, callback_update_kib, "UPDATE KIB"),
        (QUEUE_ASSIGN_PRIVILEGE, callback_assign_privilege, "ASSIGN PRIVILEGE")
    ]
    
    threads = []
    for queue, callback, name in workers:
        thread = threading.Thread(target=start_worker, args=(queue, callback, name))
        thread.daemon = True
        thread.start()
        threads.append(thread)
    
    print("\n" + "="*60)
    print("All workers started successfully!")
    print("="*60 + "\n")
    
    # Keep main thread alive
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    start_all_workers()
