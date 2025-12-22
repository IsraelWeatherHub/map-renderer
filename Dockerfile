FROM python:3.11-slim-bookworm

WORKDIR /app

# Install system dependencies for eccodes (required by cfgrib)
RUN apt-get update && apt-get install -y --fix-missing \
    libeccodes0 \
    libeccodes-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .

CMD ["python", "-u", "main.py"]
