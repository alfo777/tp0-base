FROM python:3.9.7-slim
COPY test-server /
RUN chmod 777 test-server.sh
RUN apt-get update
RUN apt-get install -y netcat
ENTRYPOINT ["/bin/sh"]