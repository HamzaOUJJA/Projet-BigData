import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    SENSORS = ["S1", "S2", "S3", "S4", "S5"]

    while true:
        data = {
            "sensor_id": random.choice(SENSORS),
            "timestamp": datetime.now().isoformat(),
            "average_speed": round(random.uniform(10, 100), 2),
            "traffic_density": round(random.uniform(0, 100), 2)
        }

        producer.send("traffic-stream", data)
        print(f"Sent: {data}")
        time.sleep(1)
    
    producer.flush()
    producer.close()