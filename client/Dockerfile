FROM golang:1.17 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
# First stage is used to compile golang binary and second stage is used to only copy the 
# binary generated to the deploy image. 
# Docker multi stage does not delete intermediate stages used to build our image, so we need 
# to delete it by ourselves. Since docker does not give a good alternative to delete the intermediate images
# we are adding a very specific label to the image to then find these kind of images and delete them
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/
COPY . .
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client


FROM busybox:latest
COPY --from=builder /build/bin/client /client
COPY ./client/config-1.yaml /config-1.yaml
COPY ./client/config-2.yaml /config-2.yaml
COPY ./client/config-3.yaml /config-3.yaml
COPY ./client/config-4.yaml /config-4.yaml
COPY ./client/config-5.yaml /config-5.yaml
COPY ./client/data/agency-1.csv /agency-1.csv
COPY ./client/data/agency-2.csv /agency-2.csv
COPY ./client/data/agency-3.csv /agency-3.csv
COPY ./client/data/agency-4.csv /agency-4.csv
COPY ./client/data/agency-5.csv /agency-5.csv
ENTRYPOINT ["/bin/sh"]