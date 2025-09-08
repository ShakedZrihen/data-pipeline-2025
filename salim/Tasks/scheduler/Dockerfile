FROM alpine:3.20

RUN apk add --no-cache docker-cli 

RUN echo "0 9 * * * docker restart crawlers >>/var/log/cron.log 2>&1" > /etc/crontabs/root

CMD ["crond", "-f", "-l", "8"]
