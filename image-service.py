import asyncio
import os 

from PIL import Image, ImageOps
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

kafka_server = "localhost:9092"  # Replace with your Redpanda server address
topic_name = "image_upload_topic"  # Replace with your topic name

request_topic = "image-request"
response_topic = "image-response"

current_dir = os.path.dirname(os.path.realpath(__file__))
images_path = os.path.join(current_dir, "static/images")

async def consume():
    consumer = AIOKafkaConsumer(
        request_topic,
        bootstrap_servers=kafka_server,
        group_id="image-process-group"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            filename = msg.value.decode('utf-8')
            await process_image(filename)
    finally:
        await consumer.stop()

async def send_to_kafka(topic, message):
    producer = AIOKafkaProducer(bootstrap_servers=kafka_server)
    await producer.start()
    try:
        await producer.send_and_wait(topic, message.encode('utf-8'))
    finally:
        await producer.stop()

async def process_image(filename):
    try:
        with Image.open(os.path.join(images_path, filename)) as img:
            grayscale = ImageOps.grayscale(img)
            new_filename = f"desaturated_{filename}"
            grayscale.save(os.path.join(images_path, new_filename))
            print(f"Processed: {new_filename}")
            # Send new filename to Kafka
            await send_to_kafka(response_topic, new_filename)
    except Exception as e:
        print(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    print("Starting image service...")
    asyncio.run(consume())