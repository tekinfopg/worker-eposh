import base64
import hashlib
import hmac
import json
import pika
import requests
import urllib3
from config import (HIKVISION_AK, HIKVISION_SIGNATURE, RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, QUEUE_NAME, HIKVISION_BASE_URL)

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_photo_as_base64(photo_url):
    """Download photo from URL and convert to base64"""
    try:
        response = requests.get(photo_url, verify=False, timeout=10)
        response.raise_for_status()
        return base64.b64encode(response.content).decode('utf-8')
    except Exception as e:
        print(f"✗ Error downloading photo from {photo_url}: {e}")
        return ""

def generate_signature(method, accept, content_type, url_path, app_secret):
    """Generate X-Ca-Signature using HMAC-SHA256"""
    text_to_sign = f"{method}\n{accept}\n{content_type}\n{url_path}"
    hash_obj = hmac.new(
        app_secret.encode('utf-8'),
        text_to_sign.encode('utf-8'),
        hashlib.sha256
    )
    signature = base64.b64encode(hash_obj.digest()).decode('utf-8')
    return signature

def send_to_hikvision(employee: dict):
    """Send single employee data to Hikvision"""
    print(f"Processing employee: {employee.get('name')} (KIB: {employee.get('kib_number')})")
    
    url = "https://10.14.41.217:5001/artemis/api/resource/v1/person/single/add"
    method = "POST"
    accept = "application/json"
    content_type = "application/json;charset=UTF-8"
    url_path = "/artemis/api/resource/v1/person/single/add"
    app_secret = "X1CLPkqZ2cm8mUv00qta"
    
    # Generate signature
    signature = generate_signature(method, accept, content_type, url_path, app_secret)
    
    headers = {
        "Content-Type": content_type,
        "Accept": accept,
        "X-Ca-Key": "44499395",
        "X-Ca-Signature": signature,
    }
    
    # Download and convert photo to base64
    photo_base64 = ""
    if "photo" in employee and employee["photo"]:
        photo_link = employee["photo"].get("link", "")
        if photo_link:
            photo_base64 = download_photo_as_base64(photo_link)
    
    payload = {
        "personCode": employee.get("identity_number"),
        "personFamilyName": " ",
        "personGivenName": employee.get("name"),
        "gender": 1,
        "orgIndexCode": "1",
        "remark": "From Eposh Induction",
        "phoneNo": employee.get("phone_number", "0000000000"),
        "email": employee.get("email", "xxx@gmail.com"),
        "faces": [
            {
                "faceData": photo_base64,
            }
        ],
        "beginTime": "2026-01-05T00:00:00+08:00",
        "endTime": "2030-12-31T23:59:59+08:00"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, verify=False)
        print(f"Response Status: {response.status_code}")
        print(f"Response Body: {response.text}")
        response.raise_for_status()
        print(f"✓ Employee {employee.get('name')} sent successfully")
        return response.json()
    except Exception as e:
        print(f"✗ Error sending employee {employee.get('name')}: {e}")
        return None

def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        response_data = message.get("data")
        
        # Extract employee list from response
        employees = response_data.get("data", [])
        pagination = response_data.get("pagination", {})
        
        print(f"\n{'='*60}")
        print(f"Received batch: Page {pagination.get('current_page')}/{pagination.get('last_page')}")
        print(f"Processing {len(employees)} employees...")
        print(f"{'='*60}\n")
        
        # Process each employee
        success_count = 0
        for employee in employees:
            try:
                send_to_hikvision(employee)
                success_count += 1
            except Exception as e:
                print(f"Failed to process employee: {e}")
                # Continue with next employee
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"\n✓ Batch completed: {success_count}/{len(employees)} employees processed\n")
        
    except Exception as e:
        print(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
def start_worker():
    print("Starting worker...")
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    )
    
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    
    print(f"Worker started. Waiting for messages on queue: {QUEUE_NAME}...")
    channel.start_consuming()
    
if __name__ == "__main__":
    start_worker()