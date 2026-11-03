# syntax=docker/dockerfile:1.7

FROM alpine:3.22 AS build

RUN apk add --no-cache build-base ca-certificates curl linux-headers

ARG BACNET_STACK_REF=91b6411d3fb1f6c54d71515c0e6709a528f47991
ARG BACNET_STACK_SHA256=55571c6c2697e38b6454e3381d2dcf56d63b807f67752ebf46b37783289c76bc

WORKDIR /src

RUN curl --fail --location --silent --show-error \
      --output /tmp/bacnet-stack.tar.gz \
      "https://github.com/bacnet-stack/bacnet-stack/archive/${BACNET_STACK_REF}.tar.gz" \
    && echo "${BACNET_STACK_SHA256}  /tmp/bacnet-stack.tar.gz" | sha256sum -c - \
    && tar --extract --gzip --file /tmp/bacnet-stack.tar.gz \
      --strip-components=1 --directory /src \
    && make server

FROM alpine:3.22

LABEL org.opencontainers.image.title="Kubling BACnet test device" \
      org.opencontainers.image.description="Local BACnet/IP interoperability fixture" \
      org.opencontainers.image.source="https://github.com/kubling-community/kubling-providers"

RUN addgroup -S bacnet && adduser -S -G bacnet bacnet

COPY --from=build /src/bin/bacserv /usr/local/bin/bacserv
COPY --from=build /src/license /licenses/bacnet-stack

ENV BACNET_IFACE=eth0 \
    BACNET_IP_PORT=47808

EXPOSE 47808/udp

USER bacnet

ENTRYPOINT ["/usr/local/bin/bacserv"]
CMD ["1234", "kubling-lab-device"]
