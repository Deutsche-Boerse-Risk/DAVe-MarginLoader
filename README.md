[![CircleCI](https://circleci.com/gh/Deutsche-Boerse-Risk/DAVe-MarginLoader.svg?style=shield)](https://circleci.com/gh/Deutsche-Boerse-Risk/DAVe-MarginLoader) [![Build Status](https://travis-ci.org/Deutsche-Boerse-Risk/DAVe-MarginLoader.svg?branch=master)](https://travis-ci.org/Deutsche-Boerse-Risk/DAVe-MarginLoader) [![Coverage Status](https://coveralls.io/repos/github/Deutsche-Boerse-Risk/DAVe-MarginLoader/badge.svg?branch=master)](https://coveralls.io/github/Deutsche-Boerse-Risk/DAVe-MarginLoader?branch=master) [![codebeat badge](https://codebeat.co/badges/1a292965-926b-4db1-a16a-46dbf966bb7e)](https://codebeat.co/projects/github-com-deutsche-boerse-risk-dave-marginloader) [![Dependency Status](https://dependencyci.com/github/Deutsche-Boerse-Risk/DAVe-MarginLoader/badge)](https://dependencyci.com/github/Deutsche-Boerse-Risk/DAVe-MarginLoader) [![SonarQube](https://sonarqube.com/api/badges/gate?key=com.deutscheboerse.risk:dave-margin-loader)](https://sonarqube.com/dashboard/index/com.deutscheboerse.risk:dave-margin-loader)

# DAVe-MarginLoader

**DAVe** is **D**ata **A**nalytics and **V**isualisation S**e**rvice. This Margin Loader microservice is
responsible for loading data from Eurex Clearing Prisma AMQP Broker into the persistent database (currently Mongo).
 
## Build

```
mvn clean package
```

The shippable artifact will be built in `target/dave-margin-loader-VERSION` directory.

## Configure

Configuration is stored in `marginloader.json` file in JSON format. It is split into several sections:

### Broker

The `broker` section configures the connection parameters for the Clearing Integration Layer AMQP broker. Margin Loader
uses AMQP 1.0 protocol version.

| Option | Explanation | Example |
|--------|-------------|---------|
| `username` | User name for PLAIN authentication | `dave` |
| `password` | Password for PLAIN authentication | `secret` |
| `hostname` | Hostname of the AMQP broker | `localhost` |
| `port` | Port where the AMQP broker is listening to PLAIN connections | 5672|
| `reconnectAttempts` | Number of connection attempts in case of failure (-1 -> infinity)| -1 |
| `reconnectTimeout` | Timeout in ms between two connection attempts | 10000 |
| `listeners` | Subsection defining queue for every model (see next table) |  |

Listeners subsection has the following format:

| Option | Explanation | Example |
|--------|-------------|---------|
| `accountMargin` | Queue name for account margin data | `broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEAccountMargin` |
| `liquiGroupMargin` | Queue name for liqui group margin data | `broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupMargin` |
| `liquiGroupSplitMargin` | Queue name for liqui group split margin data | `broadcast.PRISMA_BRIDGE.PRISMA_TTSAVELiquiGroupSplitMargin` |
| `positionReport` | Queue name for position report data | `broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPositionReport` |
| `poolMargin` | Queue name for pool margin data | `broadcast.PRISMA_BRIDGE.PRISMA_TTSAVEPoolMargin` |
| `riskLimitUtilization` | Queue name for risk limit utilization data | `broadcast.PRISMA_BRIDGE.PRISMA_TTSAVERiskLimitUtilization` |

### Mongo

The `mongo` section contains the configuration of the Mongo database where the margining data will be persisted.


| Option | Explanation | Example |
|--------|-------------|---------|
| `dbName` | Name of the database which will be used | `DAVe` |
| `connectionUrl` | Connection URL to connect to the database | `mongodb://localhost:27017/?waitqueuemultiple=20000` |

### Health Check

The `healthCheck` section contains configuration where the REST API for checking the health/readiness status of the
microservice will be published.

| Option | Explanation | Example |
|--------|-------------|---------|
| `port` | Port of the HTTP server hosting REST API | 80 |

The REST API provides two endpoints for checking the state using HTTP GET method:
- /healthz
- /readiness

## Run

Use script `start_marginloader.sh` to start the application.
