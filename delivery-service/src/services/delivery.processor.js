const logger = require('../utils/logger');
const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./messages.db');

db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY,
    traceId TEXT,
    messageId TEXT,
    channel TEXT,
    status TEXT,
    timestamp TEXT
  )`);
});

const processMessage = async (payload) => {
  const { traceId, messageId, channel, to, body } = payload;

  logger.info('Processing delivery', { traceId, channel, to });

  // Simulate 20% failure
  if (Math.random() < 0.2 && payload.retryCount < 3) {
    payload.retryCount += 1;
    logger.error('Delivery failed, will retry', { traceId, retry: payload.retryCount });
    throw new Error('Simulated failure');
  }

  await new Promise(resolve => setTimeout(resolve, 1000));

  db.run(`INSERT INTO deliveries (traceId, messageId, channel, status, timestamp) 
          VALUES (?, ?, ?, ?, ?)`, 
    [traceId, messageId, channel, 'sent', new Date().toISOString()]);

  logger.info('Message delivered successfully!', { traceId, channel, to });
};