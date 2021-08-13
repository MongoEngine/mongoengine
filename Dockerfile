FROM python:3.8-slim-buster AS build

COPY requirements-dev.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements-dev.txt
RUN pip install "pymongo>=3.4,<4.0"

WORKDIR /app
COPY . ./