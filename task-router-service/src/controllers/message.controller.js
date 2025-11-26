const { v4: uuidv4 } = require('uuid');
const Redis = require('ioredis');
const logger = require('../utils/logger');

const redis = new Redis();
const DUPLICATE_SET = 'processed_messages';

const channelToStream = {
  email: 'delivery:email',
  sms: 'delivery:sms',
  whatsapp: 'delivery:whatsapp'
};

const sendMessage = async (req, res) => {
  const traceId = uuidv4();
  const spanId = uuidv4();
  
  logger.info('Incoming message request', { traceId, body: req.body });

  const { to, subject, body, channel, messageId } = req.body;

  if (!to || !body || !channel || !messageId) {
    return res.status(400).json({ error: 'Missing fields' });
  }

  if (!['email', 'sms', 'whatsapp'].includes(channel)) {
    return res.status(400).json({ error: 'Invalid channel' });
  }

  // Deduplication
  const existed = await redis.sismember(DUPLICATE_SET, messageId);
  if (existed) {
    logger.warn('Duplicate message ignored', { traceId, messageId });
    return res.status(200).json({ status: 'duplicate_ignored', messageId });
  }

  await redis.sadd(DUPLICATE_SET, messageId);
  await redis.expire(DUPLICATE_SET, 86400); // 24h

  const payload = {
    traceId,
    messageId,
    to,
    subject: subject || '',
    body,
    channel,
    timestamp: new Date().toISOString(),
    retryCount: 0
  };

  const stream = channelToStream[channel];
  await redis.xadd(stream, '*', 'data', JSON.stringify(payload));

  logger.info('Message routed successfully', { traceId, channel, messageId });

  res.json({ status: 'queued', traceId, messageId });
};

module.exports = { sendMessage };