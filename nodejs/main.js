const winlog = require('./src/Winlog.js');
const redis = require('./src/RedisUtil.js');
const db = require('./src/DBMaria.js');
const config = require("./conf/config.js");
const define = require('./conf/define.js');

const DNInfo = () => {

    winlog.info("==================================================");
    winlog.info("= FINL Block Chain                               =");
    if(config.NODE_RULE === define.NODE_RULE.DN)
    {
        winlog.info("= [ DN Ver : " + config.VERSION_INFO.DN_VERSION + " ]                             =");
    }
    else if(config.NODE_RULE === define.NODE_RULE.DBN)
    {
        winlog.info("= [ DBN Ver : " + config.VERSION_INFO.DBN_VERSION + " ]                            =");
    }
    winlog.info("==================================================");
}

DNInfo();
db.setServerID();
db.initDatabase();
redis.setRedis();