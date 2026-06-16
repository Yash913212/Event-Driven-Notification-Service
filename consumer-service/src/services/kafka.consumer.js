const kafka = require('../config/kafka.config');
const notificationService = require('./notification.service');
require('dotenv').config();

const consumer = kafka.consumer({
  groupId: process.env.KAFKA_GROUP_ID || 'notification-consumer-group'
});

let isConnected = false;

async function startConsumer() {
  if (isConnected) return;
  
  try {
    console.log('Connecting Kafka Consumer...');
    await consumer.connect();
    isConnected = true;
    console.log('Kafka Consumer connected.');

    const topic = process.env.KAFKA_TOPIC || 'user-activity';
    await consumer.subscribe({ topic, fromBeginning: true });
    console.log(`Subscribed to topic: ${topic}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const rawValue = message.value.toString();
          console.log(`Received message on partition ${partition}:`, rawValue);
          
          let parsedEvent;
          try {
            parsedEvent = JSON.parse(rawValue);
          } catch (err) {
            console.error('Failed to parse message as JSON:', rawValue, err);
            return;
          }

          await notificationService.processActivityEvent(parsedEvent);
        } catch (error) {
          console.error('Error processing consumed message:', error);
        }
      }
    });
  } catch (error) {
    console.error('Failed to start Kafka Consumer:', error);
    throw error;
  }
}

async function stopConsumer() {
  if (!isConnected) return;
  try {
    await consumer.disconnect();
    isConnected = false;
    console.log('Kafka Consumer disconnected.');
  } catch (error) {
    console.error('Error disconnecting Kafka Consumer:', error);
  }
}

module.exports = {
  startConsumer,
  stopConsumer,
  consumer
};
