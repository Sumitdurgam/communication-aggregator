// server.js
require('dotenv').config();
require('./consumers/delivery.consumer'); // auto start

const express = require('express');
const app = express();
app.get('/health', (req, res) => res.json({ status: 'running' }));

app.listen(3001, () => console.log('Delivery Service Running on 3001'));