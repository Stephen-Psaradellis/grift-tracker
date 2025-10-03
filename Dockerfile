FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ENV DOPPLER_TOKEN=${DOPPLER_TOKEN}

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

RUN apt-get update && apt-get install -y apt-transport-https ca-certificates curl gnupg && \
    curl -sLf --retry 3 --tlsv1.2 --proto "=https" 'https://packages.doppler.com/public/cli/gpg.DE2A7741A397C129.key' | gpg --dearmor -o /usr/share/keyrings/doppler-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/doppler-archive-keyring.gpg] https://packages.doppler.com/public/cli/deb/debian any-version main" | tee /etc/apt/sources.list.d/doppler-cli.list && \
    apt-get update && \
    apt-get -y install doppler

COPY . .

ENV FLASK_APP=app.py \
    PORT=8000

CMD ["doppler", "run", "--project", "shortforge", "--config", "dev", "--", "gunicorn", "--bind", "0.0.0.0:8000", "app:create_app()"]