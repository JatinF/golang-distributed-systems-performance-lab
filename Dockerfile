# Built by the GitHub Actions docker-build job.
# A local image build was not run on the authoring workstation.
# Docker is not installed there.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY . .
RUN go build -o /out/lab ./cmd/lab

FROM debian:bookworm-slim
RUN useradd --create-home --uid 10001 lab
COPY --from=build /out/lab /usr/local/bin/lab
USER lab
EXPOSE 8096
ENTRYPOINT ["lab"]
CMD ["health"]
