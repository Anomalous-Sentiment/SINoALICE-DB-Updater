FROM python:3.11-slim-bullseye

# Set container work directory
WORKDIR /src

COPY sinoalice ./sinoalice/
COPY .env .
COPY DatabaseUpdater.py .
COPY requirements.txt .
COPY start_updater.py .

COPY docker-entrypoint .
RUN chmod +x docker-entrypoint
#RUN pip install --upgrade pip

# Install deps
RUN pip install -r ./sinoalice/requirements.txt

RUN pip install -r requirements.txt

#CMD ["python", "start_updater.py"]
CMD ["sh", "docker-entrypoint"]

#CMD if [[ ! -z "$SWAP" ]]; then fallocate -l $(($(stat -f -c "(%a*%s/10)*7" .))) _swapfile && mkswap _swapfile && swapon _swapfile && ls -hla; fi; free -m; python start_updater.py