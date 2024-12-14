from flask import Flask, request, jsonify
from flask_cors import CORS
import json

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/send-event', methods=['POST'])
def send_event():
    try:
        # รับข้อมูล JSON จาก Request Body
        events = request.json  
        
        # ถ้าข้อมูลที่ส่งมาไม่ใช่ List ให้แปลงเป็น List
        if not isinstance(events, list):  
            events = [events]

        # Debugging: Log ข้อมูลที่ได้รับใน Terminal
        print("Received events:", events)

        # ส่ง Response กลับไปหา Frontend
        return jsonify({'status': 'success', 'received_events': events}), 200
    except Exception as e:
        # Debugging: Log ข้อผิดพลาด
        print(f"Error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Main Application
if __name__ == '__main__':
    app.run(debug=True)
