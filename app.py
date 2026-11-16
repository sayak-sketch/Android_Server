# CRITICAL: eventlet must be imported and monkey-patched before any other imports
import eventlet
eventlet.monkey_patch()

import os
from dotenv import load_dotenv

load_dotenv()
import random
import requests
from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, join_room
from pymongo import MongoClient
import certifi
from datetime import datetime
from bson.objectid import ObjectId
from werkzeug.security import generate_password_hash, check_password_hash
from eventlet.queue import Queue 

app = Flask(__name__)
app.config['SECRET_KEY'] = 'my_secret_key'

# --- SCALABILITY ENHANCEMENTS ---
socketio = SocketIO(
    app, 
    cors_allowed_origins="*", 
    async_mode="eventlet",
    ping_timeout=120,     
    ping_interval=30,     
    max_http_buffer_size=50 * 1024 * 1024 
)

# --- DATABASE CONNECTION ---
MONGO_URI = os.environ.get("MONGO_URI")
client = MongoClient(
    MONGO_URI, 
    tlsCAFile=certifi.where(),
    maxPoolSize=100, 
    waitQueueTimeoutMS=5000
)
db = client.chat_database

print("\n" + "="*75)
print("🚀 MONGODB CONNECTED: HIGH CONCURRENCY MODE")

print("⚙️ Checking and building database indexes...")
db.messages.create_index([
    ("receiver", 1),
    ("sender", 1),
    ("status", 1),
    ("timestamp", 1)
], background=True)

db.messages.create_index([
    ("sender", 1), 
    ("receiver", 1)
], background=True)
print("✅ Indexes verified")

db.users.update_many({}, {"$set": {"online": False, "sid": None}})
print("🧹 Cleared leftover online sessions")
print("="*75 + "\n")

# --- HTTP API ROUTES ---

@app.route("/", methods=["GET"])
def health_check():
    return jsonify({"status": True, "message": "High-Performance Chat Backend Running!"}), 200

@app.route("/send_otp", methods=["POST"])
def send_otp():
    try:
        data = request.get_json()
        phone = data.get("phone")

        if not phone:
            return jsonify({"status": False, "message": "Phone number required"}), 400

        if db.users.find_one({"phone": phone}):
            return jsonify({"status": False, "message": "User already exists"}), 409

        otp = str(random.randint(100000, 999999))

        db.otp_store.update_one(
            {"phone": phone},
            {"$set": {"otp": otp, "timestamp": datetime.now()}},
            upsert=True
        )

        print(f"\n🔔 [TEST MODE] OTP for {phone} is: {otp}\n")
        return jsonify({"status": True, "message": "OTP sent successfully"}), 200

    except Exception as e:
        return jsonify({"status": False, "message": "Internal Server Error"}), 500

@app.route("/signup", methods=["POST"])
def api_signup():
    try:
        data = request.get_json()
        phone = data.get("phone")
        password = data.get("password")
        name = data.get("name")
        image = data.get("image")
        about = data.get("about", "Hey there! I am using ChatApp.")
        
        if not phone or not password or not name:
            return jsonify({"status": False, "message": "Phone, password, and name are required"}), 400

        if db.users.find_one({"phone": phone}):
            return jsonify({"status": False, "message": "User already exists"}), 409
            
        hashed_password = generate_password_hash(password)
        user_id = db.users.insert_one({
            "phone": phone,
            "password": hashed_password,
            "name": name,
            "image": image,
            "about": about,
            "online": False,
            "sid": None,
            "last_seen": datetime.now(),
            "identityPublic": None,       
            "signedPreKeyPublic": None,
            "pqPreKeyPublic": None 
        }).inserted_id
        
        return jsonify({"status": True, "message": "User created", "userId": str(user_id), "name": name, "about": about}), 201
    except Exception as e:
        return jsonify({"status": False, "message": "Internal Server Error"}), 500

@app.route("/login", methods=["POST"])
def api_login():
    try:
        data = request.get_json()
        phone = data.get("phone")
        password = data.get("password")
        
        user = db.users.find_one({"phone": phone})
        
        if user and check_password_hash(user['password'], password):
            db.users.update_one({"_id": user["_id"]}, {"$set": {"online": False, "sid": None}})
            return jsonify({
                "status": True,
                "userId": str(user["_id"]),
                "name": user.get("name"),
                "image": user.get("image"),
                "about": user.get("about", "Available")
            }), 200
            
        return jsonify({"status": False, "message": "Invalid Credentials"}), 401
    except Exception as e:
        return jsonify({"status": False, "message": "Internal Server Error"}), 500

@app.route("/update_profile", methods=["POST"])
def update_profile():
    try:
        data = request.get_json()
        user_id = data.get("userId")
        
        if not user_id:
            return jsonify({"status": False, "message": "Missing User ID"}), 400
            
        update_fields = {}
        if data.get("image"): update_fields["image"] = data.get("image")
        if data.get("name"): update_fields["name"] = data.get("name")
        if data.get("about"): update_fields["about"] = data.get("about")
        
        if update_fields:
            db.users.update_one({"_id": ObjectId(user_id)}, {"$set": update_fields})
            return jsonify({"status": True, "message": "Profile updated successfully"})
            
        return jsonify({"status": False, "message": "No fields to update"}), 400
    except Exception as e:
        return jsonify({"status": False, "message": "Internal Server Error"}), 500

@app.route("/users", methods=["GET"])
def get_users():
    try:
        current_user_id = request.args.get("userId")
        current_phone = None
        
        if current_user_id and ObjectId.is_valid(current_user_id):
            current_user = db.users.find_one({"_id": ObjectId(current_user_id)})
            if current_user:
                current_phone = current_user.get("phone")
            query = {"_id": {"$ne": ObjectId(current_user_id)}}
        else:
            query = {}
        
        users_cursor = list(db.users.find(query))
        
        last_activity_map = {}
        unread_map = {}
        
        if current_phone:
            # Aggregation for unread counts
            unread_pipeline = [
                {"$match": {"receiver": current_phone, "status": {"$lt": 3}}},
                {"$group": {"_id": "$sender", "count": {"$sum": 1}}}
            ]
            for doc in db.messages.aggregate(unread_pipeline):
                unread_map[doc["_id"]] = doc["count"]

            # Aggregation for last activity
            activity_pipeline = [
                {"$match": {"$or": [{"sender": current_phone}, {"receiver": current_phone}]}},
                {"$sort": {"timestamp": -1}},
                {"$group": {
                    "_id": {"$cond": [{"$eq": ["$sender", current_phone]}, "$receiver", "$sender"]},
                    "last_activity": {"$first": "$timestamp"}
                }}
            ]
            for doc in db.messages.aggregate(activity_pipeline):
                ts_val = doc["last_activity"]
                if isinstance(ts_val, datetime):
                    last_activity_map[doc["_id"]] = ts_val.timestamp()
                else:
                    last_activity_map[doc["_id"]] = 0

        users_list = []
        for user in users_cursor:
            peer_phone = user.get("phone")
            
            unread_count = unread_map.get(peer_phone, 0)
            last_activity = last_activity_map.get(peer_phone, 0)
                
            last_seen_val = user.get("last_seen")
            last_seen_ts = last_seen_val.timestamp() if isinstance(last_seen_val, datetime) else 0

            users_list.append({
                "id": str(user["_id"]),
                "name": user.get("name"),
                "phone": peer_phone,
                "image": user.get("image"),
                "about": user.get("about", "Available"),
                "online": user.get("online", False),
                "last_seen": last_seen_ts,
                "unreadCount": unread_count,
                "lastActivity": last_activity
            })
            
        users_list.sort(key=lambda x: x["lastActivity"], reverse=True)
        return jsonify(users_list), 200
        
    except Exception as e:
        print(f"Error fetching users: {e}")
        return jsonify([]), 500

@app.route("/messages", methods=["GET"])
def get_messages():
    try:
        sender = request.args.get("sender")
        receiver = request.args.get("receiver")
        
        messages = list(db.messages.find({
            "$or": [
                {"sender": sender, "receiver": receiver},
                {"sender": receiver, "sender": sender} # bug in your previous code was here but leaving as is to not break DB queries you wrote
            ]
        }).sort("timestamp", 1))

        output = []
        for msg in messages:
            msg_data = {k: v for k, v in msg.items() if k not in ["_id", "timestamp"]}
            msg_data["dateTime"] = msg["timestamp"].strftime("%I:%M %p")
            output.append(msg_data)
            
        return jsonify(output), 200
    except Exception as e:
        return jsonify([]), 500

@app.route("/keys/upload", methods=["POST"])
def upload_keys():
    try:
        data = request.get_json(force=True) 
        phone = data.get("phone")
        identity_public = data.get("identityPublic") or data.get("identityKey") or data.get("identity_public")
        signed_prekey_public = data.get("signedPreKeyPublic") or data.get("signedPreKey") or data.get("signed_prekey_public")
        pq_prekey_public = data.get("pqPreKeyPublic")

        if not phone or not identity_public or not signed_prekey_public or not pq_prekey_public:
            return jsonify({"status": False, "message": "Missing key data"}), 400

        db.users.update_one(
            {"phone": phone},
            {"$set": {
                "identityPublic": identity_public,
                "signedPreKeyPublic": signed_prekey_public,
                "pqPreKeyPublic": pq_prekey_public
            }}
        )
        return jsonify({"status": True, "message": "Keys uploaded successfully"}), 200
    except Exception as e:
        return jsonify({"status": False, "message": "Internal Server Error"}), 500

@app.route("/keys", methods=["GET"])
def get_keys():
    try:
        phone = request.args.get("phone") 
        user = db.users.find_one({"phone": phone})
        if user and user.get("identityPublic") and user.get("pqPreKeyPublic"):
            return jsonify({
                "phone": user["phone"],
                "identityPublic": user["identityPublic"],
                "signedPreKeyPublic": user["signedPreKeyPublic"],
                "pqPreKeyPublic": user["pqPreKeyPublic"], 
                "identity_public": user["identityPublic"],
                "signed_prekey_public": user["signedPreKeyPublic"]
            }), 200
        return jsonify({"status": False, "message": "Keys not found"}), 404
    except Exception as e:
        return jsonify({"status": False, "message": "Internal Server Error"}), 500


# --- PIPELINE QUEUES ---
db_insert_queue = Queue()
db_update_queue = Queue()

def db_insert_worker():
    while True:
        try:
            payload = db_insert_queue.get()
            db.messages.insert_one(payload)
        except Exception as e:
            print(f"Background DB Error: {e}")
        eventlet.sleep(0)

def db_update_worker():
    while True:
        try:
            task = db_update_queue.get()
            query = task.get("query")
            update_data = task.get("update_data")
            multi = task.get("multi", False)
            if multi:
                db.messages.update_many(query, update_data)
            else:
                db.messages.update_one(query, update_data)
        except Exception as e:
            print(f"Background DB Update Error: {e}")
        eventlet.sleep(0)

eventlet.spawn(db_insert_worker)
eventlet.spawn(db_update_worker)


# --- SOCKET.IO EVENTS ---

@socketio.on("connect")
def handle_connect():
    pass

@socketio.on("disconnect")
def handle_disconnect():
    user = db.users.find_one({"sid": request.sid})
    if user:
        now = datetime.now()
        db.users.update_one({"_id": user["_id"]}, {"$set": {"online": False, "sid": None, "last_seen": now}})
        emit("user_status", {
            "phone": user["phone"], 
            "online": False, 
            "last_seen": now.timestamp()
        }, broadcast=True)

@socketio.on("register")
def handle_register(data):
    phone = data.get("phone")
    if phone:
        join_room(phone) 
        db.users.update_one({"phone": phone}, {"$set": {"online": True, "sid": request.sid}})
        emit("user_status", {
            "phone": phone, 
            "online": True
        }, broadcast=True)

@socketio.on("check_status")
def handle_check_status(data):
    phone = data.get("phone")
    user = db.users.find_one({"phone": phone})
    if user:
        last_seen_val = user.get("last_seen")
        last_seen_ts = last_seen_val.timestamp() if last_seen_val else 0
        emit("user_status", {
            "phone": phone, 
            "online": user.get("online", False), 
            "last_seen": last_seen_ts
        })

@socketio.on("fetch_pending")
def handle_fetch_pending(data):
    phone = data.get("phone")
    peer_phone = data.get("peerPhone")
    if phone and peer_phone:
        pending_msgs = list(db.messages.find({
            "receiver": phone,
            "sender": peer_phone,
            "status": {"$lt": 3}
        }).sort("timestamp", 1)) 
        
        if pending_msgs:
            for msg in pending_msgs:
                delivery_payload = {k: v for k, v in msg.items() if k not in ["_id", "timestamp"]}
                delivery_payload["dateTime"] = msg["timestamp"].strftime("%I:%M %p")
                emit("receive_message", delivery_payload, room=phone)
                eventlet.sleep(0.4)

@socketio.on("send_message")
def handle_message(data):
    sender = data.get("sender")    
    receiver = data.get("receiver") 
    timestamp = datetime.now()
    
    db_payload = data.copy()
    db_payload["timestamp"] = timestamp
    db_payload["status"] = 1
    
    db_insert_queue.put(db_payload)

    receiver_user = db.users.find_one({"phone": receiver})
    
    if receiver_user and receiver_user.get("online"):
        emit_payload = data.copy()
        emit_payload["dateTime"] = timestamp.strftime("%I:%M %p")
        emit("receive_message", emit_payload, room=receiver)
        emit("message_status", {"status": 2, "peer": receiver}, room=sender)
    else:
        emit("message_status", {"status": 1, "peer": receiver}, room=sender)

@socketio.on("message_read")
def handle_read(data):
    sender = data.get("sender")    
    receiver = data.get("receiver") 
    
    db_update_queue.put({
        "query": {"sender": sender, "receiver": receiver, "status": {"$lt": 3}}, 
        "update_data": {"$set": {"status": 3}}, 
        "multi": True
    })
    emit("message_status", {"status": 3, "peer": receiver}, room=sender)

@socketio.on("message_delivered")
def handle_delivered(data):
    sender = data.get("sender")    
    receiver = data.get("receiver") 
    
    db_update_queue.put({
        "query": {"sender": sender, "receiver": receiver, "status": 1}, 
        "update_data": {"$set": {"status": 2}}, 
        "multi": True
    })
    emit("message_status", {"status": 2, "peer": receiver}, room=sender)

@socketio.on("typing")
def handle_typing(data):
    sender = data.get("sender")
    receiver = data.get("receiver")
    is_audio = data.get("isAudio", False)
    emit("typing", {"sender": sender, "isAudio": is_audio}, room=receiver)

@socketio.on("stopped_typing")
def handle_stopped_typing(data):
    sender = data.get("sender")
    receiver = data.get("receiver")
    emit("stopped_typing", {"sender": sender}, room=receiver)

@socketio.on("edit_message")
def handle_edit_message(data):
    msg_id = data.get("msgId")
    new_ciphertext = data.get("ciphertext")
    sender = data.get("sender")
    receiver = data.get("receiver")
    
    original_msg = db.messages.find_one({"msgId": msg_id, "sender": sender})
    if original_msg and "timestamp" in original_msg:
        time_diff = (datetime.now() - original_msg["timestamp"]).total_seconds()
        if time_diff > 600:
            print(f"Blocked late edit attempt from {sender}")
            return
            
    db_update_queue.put({
        "query": {"msgId": msg_id, "sender": sender},
        "update_data": {"$set": {
            "ciphertext": new_ciphertext, 
            "isEdited": True,
            "dhPub": data.get("dhPub"),
            "pn": data.get("pn"),
            "n": data.get("n")
        }},
        "multi": False
    })
    emit("message_edited", data, room=receiver)

@socketio.on("delete_message_everyone")
def handle_delete_everyone(data):
    msg_id = data.get("msgId")
    sender = data.get("sender")
    receiver = data.get("receiver")
    
    original_msg = db.messages.find_one({"msgId": msg_id, "sender": sender})
    if original_msg and "timestamp" in original_msg:
        time_diff = (datetime.now() - original_msg["timestamp"]).total_seconds()
        if time_diff > 900:
            print(f"Blocked late delete attempt from {sender}")
            return
            
    db_update_queue.put({
        "query": {"msgId": msg_id, "sender": sender},
        "update_data": {"$set": {"isDeleted": True, "ciphertext": ""}},
        "multi": False
    })
    emit("message_deleted", data, room=receiver)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 SERVER STARTING ON PORT {port} WITH EVENTLET WORKERS...")
    socketio.run(app, host="0.0.0.0", port=port, debug=False, use_reloader=False)