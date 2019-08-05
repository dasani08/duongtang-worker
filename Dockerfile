FROM python:3.7-alpine3.9

MAINTAINER thanh <thanh@clgt.vn>

ENV WORKER default

RUN apk update && apk add tzdata &&\
    cp /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime &&\ 
    echo "Asia/Ho_Chi_Minh" > /etc/timezone &&\ 
    apk del tzdata && rm -rf /var/cache/apk/*

WORKDIR /app
COPY requirements.txt .
RUN apk add --virtual .build-deps \
    gcc \
    musl-dev \
    libffi-dev \
    openssl-dev \
    && pip install --upgrade pip \
    && pip install cryptography==2.2.2 \ 
    && pip install --no-cache-dir -r requirements.txt \
    && apk del .build-deps

COPY start.sh ./start.sh

RUN chmod a+x ./start.sh

ENTRYPOINT ["./start.sh"]