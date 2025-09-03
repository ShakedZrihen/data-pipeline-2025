FROM alpine:3.20

# Install docker client + cron
RUN apk add --no-cache docker-cli

# Create cron job directly in the image
RUN echo "0 * * * * docker restart crawler >>/var/log/cron.log 2>&1" > /etc/crontabs/root

CMD ["crond", "-f", "-l", "8"]
