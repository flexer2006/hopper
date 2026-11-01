ARG GO_VERSION=1.27.3

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOFLAGS="-trimpath" \
    GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -buildvcs=false -ldflags="-s -w" -o /out/hopper ./cmd/hopper

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS runtime

LABEL org.opencontainers.image.title="hopper" \
      org.opencontainers.image.description="Outgoing work delivery system." \
      org.opencontainers.image.source="https://github.com/flexer2006/hopper"

WORKDIR /
COPY --from=build --chown=65532:65532 /out/hopper /hopper

USER 65532:65532

ENTRYPOINT ["/hopper"]
CMD ["api"]
