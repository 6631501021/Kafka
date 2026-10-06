FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common.py backend1.py worker.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "backend1.py"]
