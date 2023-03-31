FROM python:3.11-slim-bullseye

# Set container work directory
WORKDIR /src

COPY . .

# Install deps
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "start_updater.py"]