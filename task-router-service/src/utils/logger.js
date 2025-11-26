const winston = require('winston');
const { ecsFormat } = require('@elastic/ecs-winston-format');

const logger = winston.createLogger({
  format: ecsFormat({ convertReqRes: true }),
  transports: [
    new winston.transports.Http({
      host: 'localhost',
      port: 9200,
      path: '/_bulk',
    }),
    new winston.transports.Console()
  ]
});

module.exports = logger;