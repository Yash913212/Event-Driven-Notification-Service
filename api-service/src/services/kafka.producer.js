const kafka = require('../config/kafka.config');

const producer = kafka.producer();
let isConnected = false;

async function connectProducer() {
  if (isConnected) return;
  try {
    console.log('Connecting Kafka Producer...');
    await producer.connect();
    isConnected = true;
    console.log('Kafka Producer connected successfully.');
  } catch (error) {
    console.error('Error connecting Kafka Producer:', error);
    throw error;
  }
}

async function disconnectProducer() {
  if (!isConnected) return;
  try {
    await producer.disconnect();
    isConnected = false;
    console.log('Kafka Producer disconnected.');
  } catch (error) {
    console.error('Error disconnecting Kafka Producer:', error);
  }
}

async function publishEvent(topic, message) {
  await connectProducer();
  try {
    const payload = JSON.stringify(message);
    const result = await producer.send({
      topic: topic || 'user-activity',
      messages: [
        {
          key: message.event_id || null,
          value: payload,
        },
      ],
    });
    return result;
  } catch (error) {
    console.error('Failed to publish event to Kafka:', error);
    throw error;
  }
}

module.exports = {
  publishEvent,
  connectProducer,
  disconnectProducer,
  producer
};
