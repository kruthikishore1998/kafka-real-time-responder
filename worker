import time
import math
import random
import threading
import signal
from yadtq import YADTQ

# Kafka Configuration
KAFKA_BROKER = 'localhost:9092'
REDIS_BACKEND = 'redis://localhost:6379/0'
GROUP_ID = 'worker_group'  # Group ID for partitioned task processing

# Initialize YADTQ instance
yadtq = YADTQ(broker=KAFKA_BROKER, backend=REDIS_BACKEND)

# Unit Locations and Speeds
UNIT_LOCATIONS = {
    "medical": {"lat": 12.9716, "lon": 77.5946},
    "fire": {"lat": 12.9260, "lon": 77.6762},
    "police": {"lat": 12.9902, "lon": 77.5372}
}
AVERAGE_SPEED_KMH = {
    "medical": 50,
    "fire": 30,
    "police": 45
}




# Bangalore Geographical Bounds
BANGALORE_BOUNDS = {
    "lat_min": 12.8,
    "lat_max": 13.1,
    "lon_min": 77.5,
    "lon_max": 77.7
}

def is_location_in_bangalore(location):
    """Check if the location is within the Bangalore boundary."""
    lat, lon = location["lat"], location["lon"]
    return (BANGALORE_BOUNDS["lat_min"] <= lat <= BANGALORE_BOUNDS["lat_max"] and
            BANGALORE_BOUNDS["lon_min"] <= lon <= BANGALORE_BOUNDS["lon_max"])



# Haversine Distance Calculation
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of Earth in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# Calculate Estimated Time (ETA)
def calculate_eta(emergency_type, location, severity=None):
    unit_location = UNIT_LOCATIONS[emergency_type]
    speed_kmh = AVERAGE_SPEED_KMH[emergency_type]

    # Adjust speed for severity in medical emergencies
    if emergency_type == "medical" and severity:
        if severity == "critical":
            speed_kmh *= 1.5  # Increase speed for critical cases
        elif severity == "moderate":
            speed_kmh *= 0.75  # Decrease speed for moderate cases

    distance = haversine_distance(
        unit_location["lat"], unit_location["lon"],
        location["lat"], location["lon"]
    )
    return round((distance / speed_kmh) * 60, 2)  # Return time in minutes

# Medical Task Handler
def handle_medical(task_data):
    severity = task_data.get("severity", "moderate")
    location = task_data["location"]
    eta = calculate_eta("medical", location, severity)
    signal_code = "red" if severity == "critical" else "blue"
    time.sleep(12)  # Simulate processing time
    return {"eta": eta, "signal_code": signal_code, "severity": severity, "details": "Medical team dispatched"}

# Fire Task Handler
def handle_fire(task_data):
    priority = task_data.get("priority", "medium")
    location = task_data["location"]
    eta = calculate_eta("fire", location)
    evacuation_radius = {"high": 5, "medium": 3, "low": 1}.get(priority, 2)
    time.sleep(12)  # Simulate processing time
    return {"eta": eta, "priority": priority, "evacuation_radius": evacuation_radius, "details": "Firefighters dispatched"}

# Police Task Handler
def handle_police(task_data):
    threat_level = task_data.get("threat_level", "medium")
    location = task_data["location"]
    eta = calculate_eta("police", location)
    batches_dispatched = {"high": 5, "medium": 3, "low": 2}.get(threat_level, 1)
    time.sleep(12)  # Simulate processing time
    return {"eta": eta, "threat_level": threat_level, "batches_dispatched": batches_dispatched, "details": "Police en route"}

def process_task(task):
    """Process a single task with location validation, retries, and status updates."""
    retries = 3
    backoff = 2  # Exponential backoff factor
    task_type = task["type"]
    task_data = task["data"]
    task_id = task["task_id"]  # Extract task_id for status updates

    print(f"Processing task: {task}")
    
    # Update task status to "processing" in Redis
    yadtq._store_result(task_id, {"status": "processing"})
    print(f"Task {task_id} status updated to 'processing'.")

    # Validate location
    location = task_data.get("location")
    if not is_location_in_bangalore(location):
        error_message = f"Location coordinates {location} are outside Bangalore."
        print(f"Task {task_id} failed: {error_message}")
        
        # Update task status to "failed" in Redis and exit
        yadtq._store_result(task_id, { "status":"failed","error": error_message})
        print(f"Task {task_id} status updated to 'failed' in Redis.")
        return {"error": error_message, "details": "Task failed due to invalid location"}

    # Process task logic
    for attempt in range(1, retries + 1):
        try:
            if task_type == "medical":
                result = handle_medical(task_data)
            elif task_type == "fire":
                result = handle_fire(task_data)
            elif task_type == "police":
                result = handle_police(task_data)
            else:
                raise ValueError(f"Unknown task type: {task_type}")

            # Task completed successfully; update status in Redis
            yadtq._store_result(task_id, {"status": "success", "result": result})
            print(f"Task {task_id} successfully processed. Status updated to 'success' in Redis.")
            return result
        except Exception as e:
            print(f"Error processing task {task_id}: {e}")
            if attempt < retries:
                print(f"Retrying task {task_id} (Attempt {attempt + 1} of {retries})...")
                time.sleep(backoff ** attempt)
            else:
                print(f"Task {task_id} failed after {retries} attempts.")
                error_message = f"Task failed permanently: {str(e)}"
                yadtq._store_result(task_id, {"status": "failed", "error": error_message})
                print(f"Task {task_id} status updated to 'failed' in Redis.")
                return {"error": error_message, "details": "Task failed permanently"}

# Graceful Shutdown
def shutdown_worker(signum, frame):
    """Handle shutdown signal."""
    print(f"Shutting down worker {yadtq.worker_id}...")
    exit(0)

# Worker Execution
def run_worker():
    """Run the worker to process tasks and send heartbeats."""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_worker)
    signal.signal(signal.SIGTERM, shutdown_worker)

    # Initialize Kafka consumer for this worker
    yadtq.initialize_consumer(group_id=GROUP_ID)
    
    # Start heartbeat thread
    heartbeat_thread = threading.Thread(target=yadtq.send_heartbeat, daemon=True)
    heartbeat_thread.start()

    # Process tasks
    try:
        yadtq.process_task(process_task)
    except Exception as e:
        print(f"Error during task processing: {e}")

if __name__ == "__main__":
    run_worker()
