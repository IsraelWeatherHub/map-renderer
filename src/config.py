import os
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-maps")

REGIONS = {
    "israel": {"lon_min": 33.5, "lon_max": 36.5, "lat_min": 29.0, "lat_max": 33.5},
    "eastern_med": {"lon_min": 25.0, "lon_max": 40.0, "lat_min": 25.0, "lat_max": 40.0},
    "europe": {"lon_min": -10.0, "lon_max": 40.0, "lat_min": 25.0, "lat_max": 70.0},
    "middle_east": {"lon_min": 25.0, "lon_max": 60.0, "lat_min": 10.0, "lat_max": 45.0}
}
