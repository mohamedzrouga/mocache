# Build context is the repo root so the Python SDK can be copied in unbuilt:
#   docker build -f lab/api/Dockerfile ..
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app

COPY lab/api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# The SDK ships as source (no third-party deps) — copy it rather than pip install.
COPY sdk/python/mocache /app/mocache
COPY lab/api/app.py lab/api/seed.py /app/

EXPOSE 8000
CMD ["python", "-u", "app.py"]
