import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URL = os.getenv("MONGODB_URL")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME")

# MongoDB client (initialized at startup)
mongodb_client: AsyncIOMotorClient = None
mongodb_db = None


async def connect_to_mongodb():
    global mongodb_client, mongodb_db
    mongodb_client = AsyncIOMotorClient(MONGODB_URL)
    mongodb_db = mongodb_client[MONGODB_DB_NAME]
    print(f"Connected to MongoDB: {MONGODB_DB_NAME}")


async def close_mongodb_connection():
    global mongodb_client
    if mongodb_client:
        mongodb_client.close()
        print("Closed MongoDB connection")


def get_mongodb():
    return mongodb_db
