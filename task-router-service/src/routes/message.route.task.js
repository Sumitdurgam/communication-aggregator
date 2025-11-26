// routes/message.routes.js
const express = require('express');
const { sendMessage } = require('../controllers/message.controller');
const router = express.Router();

router.post('/send', sendMessage);

module.exports = router;