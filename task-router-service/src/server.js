require('dotenv').config();
const express = require('express');
const messageRoutes = require('./routes/message.routes');
const logger = require('./utils/logger');

const app = express();
app.use(express.json());

app.use('/api/messages', messageRoutes);

app.get('/health', (req, res) => res.json({ status: 'ok' }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logger.info('Task Router Service started', { port: PORT });
});