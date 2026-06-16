const { Kafka } = require('kafkajs');
require('dotenv').config();

const brokers = (process.env.KAFKA_BROKER_LIST || 'localhost:9092').split(',');

const kafka = new Kafka({
  clientId: 'notification-consumer-service',
  brokers: brokers,
  retry: {
    initialRetryTime: 300,
    retries: 8
  }
});

module.exports = kafka;
