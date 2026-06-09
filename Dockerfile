FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY task_queue/ ./task_queue/
COPY api/ ./api/
COPY main.py .

# Expose API port
EXPOSE 8000

# Run the application
CMD ["python", "main.py"]
