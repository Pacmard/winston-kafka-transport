# Kafka transport for Winston v3

[![NPM](https://nodei.co/npm/@pacmard/winston-kafka-transport.png)](https://nodei.co/npm/@pacmard/winston-kafka-transport/)

## Installment

Requires winston v3

```sh
npm install @pacmard/winston-kafka-transport --save

Or yarn

yarn add @pacmard/winston-kafka-transport
```

## How to use

```js
const winston = require('winston');
const KafkaTransport = require('@pacmard/winston-kafka-transport');

winston.add(
  new KafkaTransport({
    level: 'info',
    format: format.combine(...traceFormats),
    meta: {},
    kafkaOptions: {
      brokers: ['localhost:9092'],
      clientId: 'winston-kafka-logger',
    },
    topic: kafkaTopic,
    name: 'WinstonLogs',
    formatter: JSON.stringify,
  }),
);
```

## Configuration 

More flexible configuration for kafka brokers can be done in `kafkaOptions` parameter of transport.

Detailed description is available at kafka.js docs 
https://kafka.js.org/docs/introduction

## License

[Apache-2.0](https://github.com/Pacmard/winston-kafka-transport/blob/master/LICENSE)
