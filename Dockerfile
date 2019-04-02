FROM alpine:latest

MAINTAINER thanh <thanh@clgt.vn>

RUN apk add --no-cache \
    bash \
    gcc \
    musl-dev \
    python3 \
    python3-dev \
    libffi-dev \
    openssl-dev \
    libxslt-dev \
    && pip3 install --upgrade pip setuptools \
    && pip3 install gevent

COPY setup.py setup.py

RUN pip3 install -e .

ENV APP_DIR /app
ENV CELERY_LOGLEVEL info
ENV CELERY_CON 2
ENV CELERY_POOL gevent
ENV CELERY_QUEUE default
ENV CELERY_WORKER default

VOLUME [${APP_DIR}]
WORKDIR ${APP_DIR}

COPY start.sh .

ENTRYPOINT ["/app/start.sh"]

#CMD ["celery", "-A workers.${CELERY_WORKER}", "worker", "-Q ${CELERY_QUEUE}", "-l ${CELERY_LOGLEVEL}", "-P ${CELERY_POOL}", "--concurrency=${CELERY_CON}", "-Ofair"]