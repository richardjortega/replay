# Replay

This application is based documentation, [Capture Event Hubs data using Python](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-python), but has been customized to support reading JSON from blob storage.

## Requirements

- Docker (Linux containers)

## Usage

### Build container

```bash
$ docker build -t replay .
```

**NOTE**: Alternatively, you can use the pre-built image of `richardjortega/replay` if you wish not to build it.

### Run container

Run the following command to start the container. Set the following environment variables.

```bash
# Note: PATH_PREFIX is not a required environment variable, but can be used for filtering
#   PATH_PREFIX filters the results to return only blobs whose names begin with the specified prefix.

# Note2: MESSAGE_INTERVAL_MS is not a required environment variable, but can be used to set message interval speed.
#   MESSAGE_INTERVAL_MS is in milliseconds. Default is 100

$ docker run --rm -e STORAGE_ACCOUNT_NAME="<STORAGE_ACCOUNT_NAME>" \
    -e STORAGE_SAS_KEY="<STORAGE_SAS_KEY>" \
    -e STORAGE_CONTAINER_NAME="<STORAGE_CONTAINER_NAME>" \
    -e EVENT_HUB_NAMESPACE="<EVENT_HUB_NAMESPACE>" \
    -e EVENT_HUB_NAME="<EVENT_HUB_NAME>" \
    -e EVENT_HUB_SAS_NAME="<EVENT_HUB_SAS_NAME>" \
    -e EVENT_HUB_SAS_KEY="<EVENT_HUB_SAS_KEY>" \
    -e PATH_PREFIX="<PATH_PREFIX>" \
    -e MESSAGE_INTERVAL_MS="<MESSAGE_INTERVAL_MS>" \
    replay
```