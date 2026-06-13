import os
from fastapi import Request

INSTANCE_ID = os.getenv("INSTANCE_ID", "unknown")

async def add_instance_id_header(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Instance-ID"] = INSTANCE_ID
    return response