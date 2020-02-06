const config = require('./../conf/config.js');
const winston = require('winston');
require('winston-daily-rotate-file');
require('date-utils');

let winlog = winston.createLogger({
    transports: [
        new winston.transports.DailyRotateFile({
            level: config.debuglv === "development" ? "silly" : "info",
            filename: './log/DN.log', // store log on file name : system.log
            zippedArchive: false, // isCompress?
            format: winston.format.printf(
                info => `[${info.level.toUpperCase()}] [${new Date().toFormat('YYYY-MM-DD HH24:MI:SS')}] ${info.message}`)
        }),
        new winston.transports.Console({
            level: config.debuglv === "development" ? "silly" : "error",
            format : winston.format.printf(
                info => `[${info.level.toUpperCase()}] [${new Date().toFormat('YYYY-MM-DD HH24:MI:SS')}] ${info.message}`)
        })
    ]
});

module.exports = winlog;