FROM python:3

WORKDIR /usr/src/app
COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV STORAGE_ACCOUNT_NAME STORAGE_ACCOUNT_NAME
ENV STORAGE_SAS_KEY STORAGE_SAS_KEY
ENV STORAGE_CONTAINER_NAME STORAGE_CONTAINER_NAME
ENV EVENT_HUB_NAMESPACE EVENT_HUB_NAMESPACE
ENV EVENT_HUB_NAME EVENT_HUB_NAME
ENV EVENT_HUB_SAS_NAME EVENT_HUB_SAS_NAME
ENV EVENT_HUB_SAS_KEY EVENT_HUB_SAS_KEY
ENV MESSAGE_INTERVAL MESSAGE_INTERVAL

CMD ["python", "-u", "replay.py"]