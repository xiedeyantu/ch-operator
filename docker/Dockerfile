FROM golang:1.16 AS builder

RUN apt-get update && apt-get install -y apt-utils gettext-base

ENV GOPROXY https://goproxy.cn
ENV GO111MODULE on

WORKDIR $GOPATH/src/github.com/xiedeyantu/ch-operator
ADD . .
RUN go mod tidy
RUN OPERATOR_BIN=/tmp/ch-operator ./docker/go_build.sh

FROM alpine:3.10 AS operator

RUN apk add --no-cache ca-certificates
WORKDIR /
COPY --from=builder /tmp/ch-operator .

ENTRYPOINT ["/ch-operator"]