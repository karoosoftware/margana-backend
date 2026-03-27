# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app:/app/layer-root/python"

# Set work directory
WORKDIR /app

RUN groupadd --system appuser \
    && useradd --system --gid appuser \
    --create-home \
    --home-dir /home/appuser appuser

# Copy requirements file
COPY ecs/requirements.txt /app/ecs/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /app/ecs/requirements.txt

# Copy the application logic
COPY ecs/ /app/ecs/
COPY layer-root/ /app/layer-root/

RUN chown -R appuser:appuser /app
RUN mkdir -p /work && chown appuser:appuser /work

USER appuser

ENV HOME=/home/appuser \
    TMPDIR=/work

# Define the entrypoint and default command
# Using exec form for better signal handling
ENTRYPOINT ["python3", "/app/ecs/main.py"]

# Default command if no arguments are provided
CMD ["--target-week", "next"]
