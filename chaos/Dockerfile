FROM docker:cli
RUN apk add --no-cache bash curl
COPY chaos.sh /usr/local/bin/chaos.sh
RUN chmod +x /usr/local/bin/chaos.sh
CMD ["/usr/local/bin/chaos.sh"]
