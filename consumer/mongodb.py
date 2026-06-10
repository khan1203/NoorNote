import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URL = os.getenv("MONGODB_URL")
MONGODB_DB = os.getenv("MONGODB_DB")

mongodb_client: AsyncIOMotorClient = None
mongodb_db = None


async def connect_to_consumer_mongodb():
    global mongodb_client, mongodb_db
    mongodb_client = AsyncIOMotorClient(MONGODB_URL)
    mongodb_db = mongodb_client[MONGODB_DB]

    await mongodb_client.admin.command('ping')
    print(f"Consumer connected to MongoDB: {MONGODB_DB}")

    return mongodb_db


def get_mongodb():
    return mongodb_db
