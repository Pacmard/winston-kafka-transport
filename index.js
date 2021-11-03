const { Kafka } = require('kafkajs');
const Transport = require('winston-transport');
const CircularJSON = require('circular-json');
const defaultsDeep = require('lodash.defaultsdeep');
const util = require('util');
const { v4: uuidv4 } = require('uuid');

const debug = util.debuglog('winston:kafka');

const DEFAULTS = {
  topic: 'winston-kafka-logs',
  kafkaOptions: {
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
  partition: 0,
};

module.exports = class KafkaTransport extends Transport {
  constructor(options) {
    super(options);
    this.options = defaultsDeep({}, options || {}, DEFAULTS);

    this.timestamp = () => {
      return Date.now();
    };
    this.jsonformatter = options.jsonformatter || CircularJSON;

    this.connected = false;
    return this.connect();
  }

  connect() {
    this.client = new Kafka(this.options.kafkaOptions);
    this.producer = this.client.producer();
    this.producer.connect();

    const { CONNECT, REQUEST_TIMEOUT } = this.producer.events;

    this.producer.on(CONNECT, () => {
      this.connected = true;
    });

    this.producer.on(REQUEST_TIMEOUT, (err) => {
      throw new Error(err);
    });
  }

  _sendPayload(payload) {
    const { CONNECT } = this.producer.events;

    if (!this.connected) {
      return this.producer.on(CONNECT, () => this.producer.send(payload));
    }

    return this.producer.send(payload);
  }

  disconnect() {
    this.connected = false;
    return this.producer.disconnect();
  }

  log(message, callback) {
    try {
      const payload = {
        topic: this.options.topic,
        messages: [
          {
            key: uuidv4(),
            value: this.jsonformatter.stringify({ ...message, timestamp: this.timestamp() }),
            partition: this.options.partition,
          },
        ],
      };

      this._sendPayload(payload, (error) => {
        if (error) {
          debug(error);
        }
      });
      return callback(null, true);
    } catch (error) {
      return callback(error);
    }
  }
};
