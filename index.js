const { Kafka } = require('kafkajs');
const Transport = require('winston-transport');
const CircularJSON = require('circular-json');
const defaultsDeep = require('lodash.defaultsdeep');
const { v4: uuidv4 } = require('uuid');

const DEFAULTS = {
  topic: 'winston-kafka-logs',
  kafkaOptions: {
    brokers: ['localhost:9092'], // required!
    clientId: 'winston-kafka-logger',
    connectionTimeout: 10 * 1000,
    requestTimeout: 30 * 1000,
    retry: {
      maxRetryTime: 30000,
      initialRetryTime: 300,
      retries: 5,
      restartOnFailure: true,
    },
  },
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
    this.client = new Kafka(this.options.kafkaOptions);
    this.producer = this.client.producer();
  }

  async connect() {
    try {
      await this.producer.connect();

      const { CONNECT, REQUEST_TIMEOUT } = this.producer.events;

      this.producer.on(CONNECT, () => {
        console.log(`Connected to Kafka brokers: ${this.options.kafkaOptions.brokers}`);
        this.connected = true;
      });

      this.producer.on(REQUEST_TIMEOUT, (err) => {
        throw new Error(err);
      });
    } catch (err) {
      console.log(err);
    }
  }

  async _sendPayload(payload) {
    const { CONNECT } = this.producer.events;

    try {
      if (!this.connected) {
        await this.connect();
        return this.producer.on(CONNECT, async () => {
          const res = await this.producer.send(payload).catch((err) => console.log(err));
          console.log(`errorCode: ${res[0].errorCode}`);
        });
      }

      const res = await this.producer.send(payload);
      console.log(`errorCode: ${res[0].errorCode}`);
      return res;
    } catch (err) {
      console.log(err);
    }
  }

  disconnect() {
    console.log('Disconnected from kafka');
    this.connected = false;
    return this.producer.disconnect();
  }

  log(info, callback) {
    const messageBuffer = Buffer.from(this.options.formatter(info));
    const message = JSON.parse(messageBuffer.toString());

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
          console.log(error);
        }
      });
      return callback(null, true);
    } catch (error) {
      console.log(error);
      return callback(error);
    }
  }
};
