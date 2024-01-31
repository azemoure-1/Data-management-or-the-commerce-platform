from confluent_kafka import Producer
import requests
import json
import time

topic = "product"
kafka_config = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(kafka_config)

api_url = 'http://127.0.0.1:5000/get_next_rows'

# Simulate real-time data consumption indefinitely
while True:
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()

        # Produce each row to the Kafka topic and print to console
        for row in data:
            # Serialize the row as a JSON string
            serialized_row = json.dumps(row)
            
            # Produce the message to the Kafka topic
            producer.produce(topic, key=str(row.get('itemId')), value=serialized_row)
            
            # Wait for the message to be sent (optional)
            producer.poll(0)

            # Print the row to the console
            print(f"Produced to Kafka topic: {topic}, Row: {row}")

        print(f"Produced {len(data)} rows to Kafka topic: {topic}")
    else:
        print(f"Error: {response.status_code} - {response.json()['message']}")

    time.sleep(3)  # Adjust the sleep duration as needed
