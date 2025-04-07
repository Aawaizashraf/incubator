from fastapi import FastAPI, WebSocket, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Column, Float, Integer, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from typing import Optional
import json
import os

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

# === WebSocket Endpoint (/ws) ===
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
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

                db = SessionLocal()
                reading = SensorReading(temperature=temperature, humidity=humidity)
                db.add(reading)
                db.commit()
                db.close()

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
    
@app.get("/hello")
def hello():
    return {"msg": "Hello World"}