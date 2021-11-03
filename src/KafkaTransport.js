const { Kafka } = require('kafkajs');
const Transport = require('winston-transport');
const CircularJSON = require('circular-json');

const DEFAULTS = {
  topic: 'winston-kafka-logs',
  kafkaClient: {
    kafkaHost: '127.0.0.1:9092', // required!
    clientId: 'winston-kafka-logger',
    connectTimeout: 10 * 1000,
    requestTimeout: 30 * 1000,
    idleConnection: 5 * 60 * 1000,
    autoConnect: true,
    versions: {
      disabled: false,
      requestTimeout: 500,
    },
    connectRetryOptions: {
      retries: 5,
      factor: 2,
      minTimeout: 1000,
      maxTimeout: 60 * 1000,
      randomize: true,
    },
    maxAsyncRequests: 10,
    noAckBatchOptions: null,
  },
  producer: {
    partitionerType: 0, // default: 0, random: 1, cyclic: 2, keyed: 3, custom: 4
    requireAcks: 1,
    ackTimeoutMs: 100,
  },
  highWaterMark: 100,
};

module.exports = class KafkaTransport extends Transport {
  constructor(options) {
    super(options);
    this.options = { ...options };

    this.timestamp = () => {
      return Date.now();
    };
    this.jsonformatter = options.jsonformatter || CircularJSON;

    this.connected = false;
    return this.connect();
  }

  async connect() {
    this.client = new Kafka(this.options.kafkaClient);
    this.producer = this.client.producer();
    await this.producer.connect();

    const { CONNECT, REQUEST_TIMEOUT } = this.producer.events;

    this.producer.on(CONNECT, () => {
      this.connected = true;
    });

    this.producer.on(REQUEST_TIMEOUT, (err) => {
      throw new Error(err);
    });
  }

  async disconnect() {
    this.connected = false;
    await this.producer.disconnect();
  }

  log(info, callback) {
    if (this.connected) {
    } else {
    }
  }
};
