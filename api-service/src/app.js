const express = require('express');
const apiRoutes = require('./routes/api.routes');
const kafkaProducer = require('./services/kafka.producer');
const dbPool = require('./config/db.config');
require('dotenv').config();

const app = express();
const PORT = process.env.API_PORT || 3000;

// Body parser
app.use(express.json());

// Routes
app.use('/api', apiRoutes);

// Health check endpoint (verifies database availability)
app.get('/health', async (req, res) => {
  try {
    await dbPool.query('SELECT 1');
    res.status(200).json({ status: 'UP', service: 'api-service', database: 'connected' });
  } catch (error) {
    res.status(500).json({ status: 'DOWN', error: error.message });
  }
});

// Global error handler
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal Server Error' });
});

// Start server (only if not running tests)
let server;
if (process.env.NODE_ENV !== 'test') {
  server = app.listen(PORT, async () => {
    console.log(`API Service listening on port ${PORT}`);
    try {
      await kafkaProducer.connectProducer();
    } catch (err) {
      console.error('Failed to connect to Kafka on startup. Connection will be retried on demand.', err);
    }
  });

  // Graceful shutdown
  const gracefulShutdown = async () => {
    console.log('Shutting down API Service gracefully...');
    server.close(async () => {
      try {
        await kafkaProducer.disconnectProducer();
        await dbPool.end();
        console.log('API Service resources closed. Exiting.');
        process.exit(0);
      } catch (err) {
        console.error('Error during API Service shutdown:', err);
        process.exit(1);
      }
    });
  };

  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGINT', gracefulShutdown);
}

module.exports = app;
