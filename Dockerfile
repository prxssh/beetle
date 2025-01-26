# Builder stage
FROM hexpm/elixir:1.18.2-erlang-27.2.1-alpine-3.19.6 AS builder

ENV MIX_ENV=prod

# Install build dependencies
RUN apk add --no-cache build-base git

# Prepare build directory
WORKDIR /app

# Install hex + rebar
RUN mix local.hex --force && mix local.rebar --force

# Copy mix dependencies
COPY mix.exs mix.lock ./
RUN mix deps.get --only $MIX_ENV && mix deps.compile
COPY . .
RUN mix compile
RUN mix release

# Final state
FROM hexpm/elixir:1.18.2-erlang-27.2.1-alpine-3.19.6

RUN apk add --no-cache openssl libgcc libstdc++

ENV MIX_ENV=prod

COPY --from=builder /app/_build/$MIX_ENV/rel/beetle /app/beetle

WORKDIR /app/beetle

EXPOSE 6969

CMD ["./bin/beetle", "start"]
