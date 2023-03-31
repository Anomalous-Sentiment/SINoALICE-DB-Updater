FROM python:3.11-slim-bullseye

# Set container work directory
WORKDIR /src

COPY sinoalice ./sinoalice/
COPY .env .
COPY DatabaseUpdater.py .
COPY requirements.txt .
COPY start_updater.py .

RUN pip install --upgrade pip

# Install deps
RUN pip install -r ./sinoalice/requirements.txt

RUN pip install -r requirements.txt

CMD ["python", "start_updater.py"]