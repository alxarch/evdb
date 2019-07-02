FROM golang:1.12-alpine AS builder
RUN apk add --no-cache git
ENV GO111MODULES=1
WORKDIR /meterd
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build ./...
RUN go install ./cmd/meterd

FROM golang:1.12-alpine
COPY --from=builder /go/bin/meterd /meterd
EXPOSE 8080
VOLUME [ "/data" ]
ENTRYPOINT [ "/meterd" ]
CMD [ "-db", "file:///data" ]