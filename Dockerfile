# Multi-stage build shared by all three services. The target binary is selected
# with the SERVICE build arg (trip | payment | booking), which maps to a cmd/ dir.
#
# Corporate-TLS note: Go uses the OS system certificate store, and the golang
# base image bundles the Mozilla CA bundle, so `go mod download` validates the
# Go module proxy's certificate inside the container on most networks. If your
# network performs TLS inspection with a private root CA, mount or COPY that CA
# into /usr/local/share/ca-certificates and run `update-ca-certificates` in the
# build stage, or build the binaries on the host and COPY them in. Never disable
# certificate verification.
FROM golang:1.22-alpine AS build
WORKDIR /src

# Cache dependencies first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
RUN test -n "$SERVICE" || (echo "SERVICE build arg is required" && false)
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
