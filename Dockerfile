FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System dependencies for camelot, OpenCV and fonts
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ghostscript \
    libffi-dev \
    libglib2.0-0 \
    libgl1 \
    libgomp1 \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV FLASK_APP=app.py \
    PORT=8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "app:create_app()"]
