# Load generator: standard library only, same rule as the cache node and SDKs.
FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY bench/bench.py /app/bench.py
ENTRYPOINT ["python", "-u", "/app/bench.py"]
