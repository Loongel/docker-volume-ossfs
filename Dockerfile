FROM golang:1.14.4-alpine3.12 as builder
COPY . /go/src/github.com/loongel/docker-volume-ossfs
WORKDIR /go/src/github.com/loongel/docker-volume-ossfs
RUN set -ex \
    && apk add --no-cache --virtual .build-deps gcc libc-dev \
    && go install --ldflags '-extldflags "-static"' \
    && apk del .build-deps
CMD ["/go/bin/docker-volume-ossfs"]

FROM alpine:3.12
RUN apk add --no-cache fuse alpine-sdk automake autoconf libxml2-dev fuse-dev curl-dev \
  && wget -qO - https://github.com/aliyun/ossfs/archive/v1.80.6.zip |unzip -q - \
  && mv ossfs-1.80.6 ossfs-master \
  && cd ossfs-master \
  && sh ./autogen.sh \
  && sh ./configure --prefix=/usr \
  && make \
  && make install \
  && rm -rf /var/cache/apk/* \
  && cd .. && rm -rf ossfs-master
RUN mkdir -p /run/docker/plugins /mnt/state /mnt/volumes

COPY --from=builder /go/bin/docker-volume-ossfs .
CMD ["docker-volume-ossfs"]


