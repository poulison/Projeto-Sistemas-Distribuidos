FROM python:3.12-slim
RUN pip install --no-cache-dir pyzmq==25.1.2 msgpack==1.0.8
WORKDIR /app
COPY reference.py .
CMD ["python", "reference.py"]
