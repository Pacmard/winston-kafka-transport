const { Kafka } = require('kafkajs');
const Transport = require('winston-transport');
const CircularJSON = require('circular-json');
const defaultsDeep = require('lodash.defaultsdeep');
const { v4: uuidv4 } = require('uuid');

const { LOGS_KAFKA_LOG_DELIVERY_STATUS } = process.env;

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

  logSendingState(res) {
    if (LOGS_KAFKA_LOG_DELIVERY_STATUS) {
      if (res[0].errorCode === 0) {
        console.log('Log successfully sent to Kafka');
      } else {
        console.log(
          `An error occurred while sending log to kafka\nError code: ${res[0].errorCode}`,
        );
      }
    }
  }

  async _sendPayload(payload) {
    const { CONNECT } = this.producer.events;

    try {
      if (!this.connected) {
        await this.connect();
        return this.producer.on(CONNECT, async () => {
          const res = await this.producer.send(payload).catch((err) => console.log(err));
          this.logSendingState(res);
        });
      }

      const res = await this.producer.send(payload);
      this.logSendingState(res);
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
            value: typeof message === 'object' ? this.jsonformatter.stringify(message) : message,
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
