# DAVe Margin Loader Docker image

**DAVe Margin Loader** docker image allows DAVe Margin Loader to be executed in Docker / Kubernetes. It contains an entrypoint which will take case of the configuration based on environment variables. The different options are described below.

## Examples

To run DAVe Margin Loader in Docker, you have to pass the environment variables to the `docker run` command.

`docker run -ti -P -e DAVE_HTTP_SSL_CERT="$webCrt" -e DAVE_HTTP_SSL_KEY="$webKey" dbgdave/dave-store-manager:latest`

## Options

### General

| Option | Explanation | Example |
|--------|-------------|---------|
| `JAVA_OPTS` | JVM options | `-Xmx=512m` |


### Logging

Allows to configure logging parameters. Supported log levels are `off`, `error`, `warn`, `info`, `debug`, `trace` and `all`.

| Option | Explanation | Example |
|--------|-------------|---------|
| `DAVE_LOG_LEVEL` | Logging level which should be used | `info` |


### AMQP Broker

| Option | Explanation | Example |
|--------|-------------|---------|
| `DAVE_BROKER_USERNAME` | Name of the user used when connecting to the broker |  |
| `DAVE_BROKER_PASSWORD` | Password for the user used when connecting to the broker |  |
| `DAVE_BROKER_PORT` | Port where the broker is listening to PLAIN connections |  |

### Store Manager

| Option | Explanation | Example |
|--------|-------------|---------|
| `DAVE_STOREMANAGER_HOSTNAME` | Hostname where DAVe Store Manager is running |  |
| `DAVE_STOREMANAGER_PORT` | Port where DAVe Store Manager is listening to SSL connections |  |
| `DAVE_STOREMANAGER_CLIENT_SSL_KEY` | Private key of the client in PEM format |  |
| `DAVE_STOREMANAGER_CLIENT_SSL_CERT` | Public key of the HTTP server in CRT format |  |
| `DAVE_STOREMANAGER_CLIENT_SSL_REQUIRE_AUTH` | Make SSL Client Authentication required | `true` |
| `DAVE_STOREMANAGER_VERIFY_HOST` | Require verification of the DAVe Store Manager host name during SSL | `true` |
| `DAVE_STOREMANAGER_SSL_TRUST_CERTS` | List of DAVe Store Manager's public certificate | |
