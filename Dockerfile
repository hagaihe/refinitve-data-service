# Use a multi-stage build to minimize the final image size
FROM python:3.9-slim AS builder

# Set the working directory
WORKDIR /app

# Copy only the requirements file to leverage caching
COPY requirements.txt /app/

# Install the dependencies in a virtual environment
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache

# Copy the application code
COPY . /app

# Copy the virtual environment to the final image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /opt/venv /opt/venv

# Ensure the virtual environment is used
ENV PATH="/opt/venv/bin:$PATH"

# Copy the application code
COPY . /app

# Expose the port
EXPOSE 8080

# Set environment variable
ENV NAME RefinitivDataService

# Run the application
CMD ["python", "main.py"]
