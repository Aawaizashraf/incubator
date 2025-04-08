from fastapi import FastAPI, HTTPException, WebSocket, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Column, Float, Integer, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Optional
import json
import os
from zoneinfo import ZoneInfo  # native timezone support (Python 3.9+)
import zoneinfo

# === PostgreSQL Config ===
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

Base = declarative_base()

class SensorReading(Base):
    __tablename__ = 'sensor_data'
    id = Column(Integer, primary_key=True)
    temperature = Column(Float)
    humidity = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)

# Create database engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(bind=engine)

# === FastAPI App ===
app = FastAPI()

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global shared variable to store latest reading
latest_reading = {
    "temperature": None,
    "humidity": None,
    "timestamp": None  # Full UTC timestamp of when this was received
}

# print(zoneinfo.available_timezones())
# print(datetime.now(ZoneInfo('Asia/Kolkata')))

# === WebSocket Endpoint (/ws) ===
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global latest_reading
    await websocket.accept()
    print("WebSocket connected")

    try:
        while True:
            data = await websocket.receive_text()
            print(f"Received: {data}")

            try:
                parsed = json.loads(data)
                temperature = float(parsed.get("temperature"))
                humidity = float(parsed.get("humidity"))
                now = datetime.now(ZoneInfo('Asia/Kolkata'))

                # ✨ Keep in-memory record of the latest payload
                latest_reading = {
                    "temperature": temperature,
                    "humidity": humidity,
                    "timestamp": now
                }
                print(f"✅ Cached reading at {now.isoformat()}")

            except Exception as e:
                print(f"Error processing data: {e}")

    except Exception as e:
        print(f"WebSocket disconnected: {e}")

# === REST API: Get Data with Optional Date Filter ===
@app.get("/data")
def get_data(
    start: Optional[str] = Query(None, description="Start datetime in ISO format (e.g., 2024-06-01T00:00:00)"),
    end: Optional[str] = Query(None, description="End datetime in ISO format (e.g., 2024-06-10T23:59:59)")
):
    db = SessionLocal()
    query = db.query(SensorReading)
    try:
        if start:
            start_dt = datetime.fromisoformat(start)
            query = query.filter(SensorReading.timestamp >= start_dt)
        if end:
            end_dt = datetime.fromisoformat(end)
            query = query.filter(SensorReading.timestamp <= end_dt)

        results = query.order_by(SensorReading.timestamp).all()
        db.close()

        return [
            {
                "id": r.id,
                "temperature": r.temperature,
                "humidity": r.humidity,
                "timestamp": r.timestamp.isoformat()
            }
            for r in results
        ]
    except Exception as e:
        db.close()
        return {"error": str(e)}

@app.post("/log-latest")
def log_latest_reading():
    global latest_reading

    if not latest_reading["temperature"] or not latest_reading["humidity"]:
        raise HTTPException(status_code=400, detail="No recent reading available.")

    # Align to minute in IST
    ist_now = datetime.now(ZoneInfo('Asia/Kolkata'))
    aligned_ist_minute = ist_now.replace(second=0, microsecond=0)

    try:
        db = SessionLocal()
        entry = SensorReading(
            temperature=latest_reading["temperature"],
            humidity=latest_reading["humidity"],
            timestamp=aligned_ist_minute
        )
        db.add(entry)
        db.commit()
        db.close()
        print(f"✅ Saved reading to DB at {aligned_ist_minute.isoformat()}")

        return {
            "status": "success",
            "saved_at": aligned_ist_minute.isoformat(),
            "data": latest_reading
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.get("/healthcheck")
def hello():
    return {"Status": "Running"}