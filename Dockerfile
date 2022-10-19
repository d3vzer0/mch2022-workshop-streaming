FROM python:3.9-slim AS build

RUN adduser --system nonroot
USER nonroot

# Set home directory and virtual env path
WORKDIR /home/nonroot/app
ENV VIRTUAL_ENV=/home/nonroot/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy and install project requirements
COPY requirements.txt .
RUN python3 -m venv $VIRTUAL_ENV && \
    $VIRTUAL_ENV/bin/pip install --upgrade pip && \
    $VIRTUAL_ENV/bin/pip install -r requirements.txt

COPY streaming streaming

ENTRYPOINT ["faust", "-A", "streaming.main", "worker"]
