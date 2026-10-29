FROM rust:1.97-slim-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# aws-sdk-s3 needs ~2+ GB RAM to compile; on small builders use --build-arg JOBS=1.
ARG JOBS
RUN cargo build --release --locked ${JOBS:+--jobs $JOBS}

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/s3ync /usr/local/bin/s3ync
# Mount the config at /etc/s3ync/config.toml and keep state_dir (default /var/lib/s3ync) on a volume.
VOLUME /var/lib/s3ync
ENTRYPOINT ["s3ync"]
CMD ["run"]
