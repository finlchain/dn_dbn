const config = require('./../conf/config.js');
const define = require('./../conf/define.js');
const winlog = require('./Winlog.js');
const common = require('./Common.js');
const db = require('./DBMaria.js');
const redis = require('redis');
const addon = require('./addon/build/Release/ADDON');

const redisConfig = {
    host : "localhost",
    port : process.env.RedisPort,
    password : addon.AESDecryptPw(process.env.aes_seed_path, "key/me/redis_enc_pw")
}

let CmdNotiSub;
let CmdNotiAcksPub;

const redisChannelCheckCallbackPromise = async (redisClient, channel) => {
    return new Promise((resolve, reject) => {
        redisClient.pubsub("CHANNELS", channel, (err, replies) => {
            if(err) {
                reject(err);
            } else {
                resolve(replies);
            }
        });
    });
}

const redisChannelCheck = async (redisClient, channel) => {
    let res = await redisChannelCheckCallbackPromise(redisClient, channel).then((resData) => {
        return resData;
    })

    if(res.length === 0) {
        winlog.error("DN Process Exit : Redis Channel Check Error")
        process.exit(0);
    }
}

let readCmdNoti = async (message) => {
    let commands = common.stringSplit(message, define.REGEX.WHITE_SPACE_REGEX);
    let ack_msg = "";

    winlog.debug(commands.length);
    winlog.debug(commands);
    if(commands[define.CMD_INDEX.CMD_SEPARATOR_INDEX] === "replication")
    {
        winlog.info("Replication Setting Command");
        if(commands.length < define.CMD_ARGC.REPLICATION_CMD_ARGC)
        {
            winlog.error("Too few arguments for Replication Setting");
            ack_msg = "replication set fail";
        }

        if(commands[define.CMD_INDEX.REPLICATION_CMD_SEPARATOR_INDEX] === "set")
        {   
            if(!common.checkIP(commands[define.CMD_INDEX.REPLICATION_CMD_IP_INDEX])) 
            {
                winlog.error("Replication Master IP address is invalid");
                ack_msg = "replication set fail";
            } 
            else {
                let res = await db.slaveReplication(
                    commands[define.CMD_INDEX.REPLICATION_CMD_IP_INDEX], 
                    commands[define.CMD_INDEX.REPLICATION_CMD_MY_ROLE_INDEX], 
                    commands[define.CMD_INDEX.REPLICATION_CMD_FILE_NAME_INDEX], 
                    commands[define.CMD_INDEX.REPLICATION_CMD_LOG_FILE_POSITION_INDEX], 
                    commands[define.CMD_INDEX.REPLICATION_CMD_SUBNET_ROOT_INDEX]);

                if(res.res) {
                    winlog.info("Replication Setting Success");
                    ack_msg = "replication set complete";
                } else {
                    winlog.error(res.reason);
                    winlog.error("Replication Setting Fail");
                    ack_msg = "replication set fail";
                }
            }
        }
        else 
        {
            winlog.error("Unknown Replication command");
            ack_msg = "replication set fail";
        }
    }
    else if(commands[define.CMD_INDEX.CMD_SEPARATOR_INDEX] === "shard")
    {
        winlog.info("Shard Setting Command");
        if(commands.length < define.CMD_ARGC.SHARD_USER_CREATE_CMD_ARGC)
        {
            winlog.error("Too few arguments for Shard Setting");
            ack_msg = "shard set fail";
        }

        if(commands[define.CMD_INDEX.SHARD_CMD_SEPARATOR_INDEX] === "user")
        {
            if(commands[define.CMD_INDEX.SHARD_CMD_USER_CMD_SEPARATOR_INDEX] === "create")
            {
                let res = await db.createClient(
                    commands[define.CMD_INDEX.SHARD_CMD_USER_ID_INDEX], 
                    commands[define.CMD_INDEX.SHARD_CMD_USER_PWD_INDEX], 
                    define.DB_USER_PURPOSE.SHARD_PURPOSE);

                if(res.res) {
                    winlog.info("Create Shard Client User Success");
                    ack_msg = "shard create user complete";
                } else {
                    winlog.error("Create Shard Client User Fail");
                    ack_msg = "shard create user fail";
                }
            }
        } 
        else {
            winlog.error("Unknown Shard command");
            ack_msg = "shard set fail";
        }
    }
    else if (commands[define.CMD_INDEX.CMD_SEPARATOR_INDEX] === "leave")
    {
        if(commands[define.CMD_INDEX.LEAVE_CMD_SEPERATOR_INDEX] === "all")
        {
            let res = await db.slaveReset();

            if(res.res) {
                winlog.info("Replication Reset Success");
                ack_msg = "leave all complete";
            } else {
                winlog.error("Replication Reset Fail");
                ack_msg = "leave all fail";
            }
        }
    }
    else {
        winlog.error("Unknown Command");
        ack_msg = "unknown command";
    }
    writeCmdNotiAcks(ack_msg);
}

let writeCmdNotiAcks = async (message) => {
    try {
        if(config.redis_pubsub_check)
        {
            redisChannelCheck(CmdNotiAcksPub, "CmdNotiAcks");
        }
        CmdNotiAcksPub.publish("CmdNotiAcks", message);
        return true;
    } catch (err) {
        return false;
    }
}

module.exports.setRedis = async () => {
    winlog.info("Redis Set");
    CmdNotiSub = redis.createClient(redisConfig);
    CmdNotiAcksPub = redis.createClient(redisConfig);

    CmdNotiSub.on("message", async(channel, message) => {
        await readCmdNoti(message);
    });
    CmdNotiSub.subscribe("CmdNoti");

    if(config.redis_pubsub_check)
    {
        redisChannelCheck(CmdNotiAcksPub, "CmdNotiAcks");
    }
    let start_msg;
    if(config.NODE_RULE === define.NODE_RULE.DN)
    {
        start_msg = "DN Start";
    }
    else if (config.NODE_RULE === define.NODE_RULE.DBN)
    {
        start_msg = "DBN Start";
    }
    CmdNotiAcksPub.publish("CmdNotiAcks", start_msg);
}
