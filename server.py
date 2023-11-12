import asyncio
import os 

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

kafka_server = "localhost:9092"  # Replace with your Redpanda server address
request_topic = "image-request"
reply_topic = "image-reply"

class ImageProcessor:
    def __init__(self):
        self.producer = None
        self.consumer = None
        
    async def create_producer(self):
        self.producer = AIOKafkaProducer(bootstrap_servers=[kafka_server])
        await self.producer.start()

    async def create_consumer(self):
        self.consumer = AIOKafkaConsumer(
            reply_topic,
            group_id="image-reply-group"
        )
        await self.consumer.start()

    async def send_to_topic(self, topic, message):
        await self.producer.send_and_wait(topic, message.encode('utf-8'))

    async def consume_from_topic(self, topic, callback):
        print(f"Consuming from {topic}")
        async for msg in self.consumer:
            print(f"Received message: {msg.value.decode('utf-8')}")
            await callback(msg)

processor = ImageProcessor()

# Startup event handler
@asynccontextmanager
async def startup(*args):
    await processor.create_producer()
    await processor.create_consumer()
    yield

app = FastAPI(lifespan=startup)

# Mount the static directory to serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Redirect root URL to the static index.html
@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

# Endpoint to upload an image
@app.post("/upload-image/")
async def upload_image(file: UploadFile = File(...)):
    # Save the file
    current_dir = os.path.dirname(os.path.realpath(__file__))
    file_location = os.path.join(current_dir, f"static/images/{file.filename}")
    print(f"Saving file to {file_location}")
    with open(file_location, "wb") as file_object:
        file_object.write(await file.read())
    # Send filename to Redpanda
    await processor.send_to_topic(request_topic, file.filename)
    return {"filename": file.filename}

# WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    async def send_message_to_websocket(msg):
        await websocket.send_text(msg.value.decode('utf-8'))

    # Start consuming
    asyncio.create_task(processor.consume_from_topic(reply_topic, send_message_to_websocket))

    # Keep the connection open
    while True:
        await asyncio.sleep(10)



