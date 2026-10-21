FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY config ./config
RUN cargo build --release -p atp_runtime --bins

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data /config
COPY --from=build /src/target/release/engine /usr/local/bin/engine
COPY --from=build /src/target/release/replay /usr/local/bin/replay
COPY --from=build /src/target/release/killswitch /usr/local/bin/killswitch
COPY --from=build /src/target/release/simbroker /usr/local/bin/simbroker
COPY config /config
EXPOSE 18420
ENTRYPOINT ["engine"]
CMD ["--demo", "--mode", "paper", "--hold-secs", "86400", "--http", "0.0.0.0:18420", "--journal", "/data/demo.jsonl", "--instruments", "/config/instruments.toml", "--risk", "/config/risk.toml", "--otel", "http://otel:4318"]
