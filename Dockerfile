FROM python:3.12
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libnss3 \
    libcurl4 \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir curl_cffi==0.15.0
RUN pip install --no-cache-dir -r requirements.txt
COPY spider_core/ .
RUN mkdir -p /app/data
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
