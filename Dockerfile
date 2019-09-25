FROM python:3.7-alpine3.9 as base
LABEL maintainer="thanh <thanh@clgt.vn>"
MAINTAINER thanh <thanh@clgt.vn>

RUN apk update && apk add --no-cache \
    gcc \
    musl-dev \
    libffi-dev \
    openssl-dev \
    && pip install --upgrade pip

FROM base

ENV WORKER default
ENV APP_DIR /app

WORKDIR ${APP_DIR}

COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .

RUN chmod a+x ./start.sh
ENTRYPOINT ["./start.sh"]