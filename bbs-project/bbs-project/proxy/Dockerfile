FROM python:3.12-slim
RUN pip install --no-cache-dir pyzmq==25.1.2
WORKDIR /app
COPY proxy.py .
CMD ["python", "proxy.py"]
