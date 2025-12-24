FROM python:3.11-slim-bookworm

WORKDIR /app

# Install system dependencies for eccodes (required by cfgrib) and cartopy
RUN apt-get update && apt-get install -y --fix-missing \
    libeccodes0 \
    libeccodes-dev \
    libgeos-dev \
    libproj-dev \
    proj-bin \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download map data
COPY src/preload_maps.py .
RUN python preload_maps.py && rm preload_maps.py

COPY src/ .

CMD ["python", "-u", "main.py"]
