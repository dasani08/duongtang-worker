FROM python:3.7-alpine3.9

MAINTAINER thanh <thanh@clgt.vn>

RUN apk add --no-cache \
    bash \
    gcc \
    musl-dev \
    libffi-dev \
    libxslt-dev \
    openssl-dev \
    && pip install --upgrade pip setuptools

COPY requirement.txt requirement.txt
RUN pip install -r requirement.txt

ENV APP_DIR /app
ENV WORKER default

VOLUME [${APP_DIR}]
WORKDIR ${APP_DIR}

COPY start.sh .

ENTRYPOINT ["/app/start.sh"]