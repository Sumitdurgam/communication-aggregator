// consumers/delivery.consumer.js
const Redis = require('ioredis');
const redis = new Redis();
const { processMessage } = require('../services/delivery.processor');
const logger = require('../utils/logger');

const channels = ['delivery:email', 'delivery:sms', 'delivery:whatsapp'];

const consume = async () => {
  for (const stream of channels) {
    const messages = await redis.xreadgroup('GROUP', 'delivery-group', 'consumer-1', 'COUNT', 1, 'BLOCK', 3000, 'STREAMS', stream, '>');
    
    if (messages) {
      for (const [streamName, entries] of messages) {
        for (const [id, fields] of entries) {
          const data = fields[fields.indexOf('data') + 1];
          const payload = JSON.parse(data);

          try {
            await processMessage(payload);
            await redis.xack(streamName, 'delivery-group', id);
          } catch (err) {
            // Retry logic: requeue with delay
            if (payload.retryCount < 3) {
              setTimeout(() => {
                redis.xadd(streamName, '*', 'data', JSON.stringify(payload));
              }, 5000 * (payload.retryCount + 1));
            }
            await redis.xack(streamName, 'delivery-group', id); // or dead letter
          }
        }
      }
    }
  }
  setImmediate(consume);
};

// Create consumer group
channels.forEach(stream => {
  redis.xgroup('CREATE', stream, 'delivery-group', '$', 'MKSTREAM').catch(() => {});
});

setTimeout(() => {
  logger.info('Delivery Consumer Started');
  consume();
}, 3000);