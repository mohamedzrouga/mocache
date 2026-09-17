FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
# The real client library, pinned: this test is only meaningful if nothing
# MoCache-specific sits between it and the wire.
RUN pip install --no-cache-dir "redis>=5.2,<7"
COPY py/check.py /app/check.py
ENTRYPOINT ["python", "-u", "/app/check.py"]
