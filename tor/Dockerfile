FROM alpine:3.20

RUN apk add --no-cache tor curl netcat-openbsd

COPY torrc /etc/tor/torrc
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER tor
EXPOSE 9050 9051

ENTRYPOINT ["/entrypoint.sh"]
