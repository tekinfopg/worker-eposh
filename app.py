from flask import Flask, request, jsonify
from config import HIKVISION_AK, HIKVISION_BASE_URL, HIKVISION_SIGNATURE
from rabbitmq import publish_message
from worker import send_to_hikvision
import requests
import urllib3

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

@app.route("/send-to-hikvision", methods=["POST"])
def send_to_hikvision_route():
    payload = request.json

    if not payload:
        return jsonify({"error": "Invalid payload"}), 400

    message = {
        "event": "HIKVISION_SYNC",
        "data": payload
    }

    publish_message(message)

    return jsonify({
        "status": "queued",
        "message": "Data sent to RabbitMQ"
    }), 202
    
@app.route("/eposh-induction", methods=["POST"])
def endpointEposh():
    try:
        base_url = "https://gcp-api.eposh.id/v1/induction/employees"
        headers = {
            "x-api-key": "4237aa07-376f-4db9-9c83-9cf91dc6438f",
            "x-app-id": "hcpvision",
        }
        
        # Fetch first page to get pagination info
        params = {
            "induction_date": "2026-01-05",
            "include_base64": "false",
            "page": 1,
            "limit": 10
        }
        
        response = requests.get(base_url, headers=headers, params=params, verify=False)
        response.raise_for_status()
        first_page = response.json()
        
        pagination = first_page.get("pagination", {})
        total_pages = pagination.get("last_page", 1)
        total_records = pagination.get("total", 0)
        
        print(f"Found {total_records} employees across {total_pages} pages")
        
        # Send first page to RabbitMQ
        message = {
            "event": "HIKVISION_SYNC",
            "data": first_page
        }
        publish_message(message)
        
        # Send remaining pages to RabbitMQ
        for page in range(2, total_pages + 1):
            params["page"] = page
            response = requests.get(base_url, headers=headers, params=params, verify=False)
            response.raise_for_status()
            page_data = response.json()
            
            message = {
                "event": "HIKVISION_SYNC",
                "data": page_data
            }
            publish_message(message)
            print(f"Page {page}/{total_pages} queued")
        
        return jsonify({
            "status": "queued",
            "message": f"All {total_pages} pages ({total_records} employees) sent to RabbitMQ for processing"
        }), 202
        
    except Exception as e:
        print(f"Error in eposh-induction endpoint: {e}")
        return jsonify({"error": str(e)}), 500
  
if __name__ == "__main__":
    app.run(debug=True, port=5000)