const dbPool = require('./config/db.config');
const kafkaConsumer = require('./services/kafka.consumer');

async function testDbConnection(retries = 15, delayMs = 3000) {
  for (let i = 0; i < retries; i++) {
    try {
      await dbPool.query('SELECT 1');
      console.log('Database connection verified successfully.');
      return;
    } catch (err) {
      console.warn(`Database connection attempt ${i + 1} failed: ${err.message}. Retrying in ${delayMs / 1000}s...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  throw new Error('Could not connect to database after multiple attempts');
}

async function start() {
  try {
    console.log('Starting Notification Consumer Service...');
    
    // Verify database connection first
    await testDbConnection();

    // Start Kafka consumer
    await kafkaConsumer.startConsumer();
    
    console.log('Consumer Service is running and listening for events.');
  } catch (error) {
    console.error('Fatal error during startup:', error);
    process.exit(1);
  }
}

// Graceful shutdown
async function gracefulShutdown() {
  console.log('Shutting down Consumer Service gracefully...');
  try {
    await kafkaConsumer.stopConsumer();
    await dbPool.end();
    console.log('Consumer Service resources closed. Exiting.');
    process.exit(0);
  } catch (err) {
    console.error('Error during Consumer Service shutdown:', err);
    process.exit(1);
  }
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

start();
