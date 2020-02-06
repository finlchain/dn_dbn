const config = require('./../conf/config.js');
const define = require('./../conf/define.js');
const mysql = require('mysql2/promise');
const winlog = require('./Winlog.js');
const common = require('./Common.js');
const command = require('./Command.js');
const addon = require('./addon/build/Release/ADDON');

let server_id;

const db_config = {
    host : process.env.DBHost,
    user : process.env.DBUser,
    password : addon.AESDecryptPw(process.env.aes_seed_path, "key/me/mysql_enc_pw"),
}

const sc_db_config = {
    host : process.env.DBHost,
    user : process.env.DBUser,
    password : addon.AESDecryptPw(process.env.aes_seed_path, "key/me/mysql_enc_pw"),
    database : config.NODE_RULE === define.NODE_RULE.DN ? define.DB_NAME.DN_SC : define.DB_NAME.SC,
    supportBigNumbers : true,
    bigNumberStrings : true
}

const blk_db_config = {
    host : process.env.DBHost,
    user : process.env.DBUser,
    password : addon.AESDecryptPw(process.env.aes_seed_path, "key/me/mysql_enc_pw"),
    database : config.NODE_RULE === define.NODE_RULE.DN ? define.DB_NAME.DN_BLOCK : define.DB_NAME.BLOCK,
    supportBigNumbers : true,
    bigNumberStrings : true
}

const createTables = {
    querys : [
        "CREATE OR REPLACE TABLE `contents` ("
        + "`subnet_id` smallint(5) unsigned DEFAULT NULL,"
        + "`blk_num` bigint(20) unsigned NOT NULL,"
        + "`sig_type` tinyint(3) unsigned DEFAULT NULL,"
        + "`p2p_addr` bigint(20) unsigned DEFAULT NULL,"
        + "`bgt` bigint(20) unsigned DEFAULT NULL,"
        + "`pbh` text DEFAULT NULL,"
        + "`tx_cnt` int(11) unsigned DEFAULT NULL,"
        + "`blk_hash` text DEFAULT NULL,"
        + "`sig` text DEFAULT NULL,"
        + "PRIMARY KEY (`blk_num`)"
        + ") ENGINE=InnoDB",
        "CREATE OR REPLACE TABLE `info` ("
        + "`subnet_id` smallint(5) unsigned DEFAULT NULL,"
        + "`blk_num` bigint(20) unsigned DEFAULT NULL,"
        + "`status` tinyint(3) unsigned DEFAULT NULL,"
        + "`bct` bigint(20) unsigned DEFAULT NULL,"
        + "KEY `blk_num` (`blk_num`)"
        + ") ENGINE=InnoDB",
        "CREATE OR REPLACE TABLE `txs` ("
        + "`subnet_id` smallint(5) unsigned DEFAULT NULL,"
        + "`blk_num` bigint(20) unsigned DEFAULT NULL,"
        + "`db_key` bigint(20) unsigned DEFAULT NULL,"
        + "`sc_hash` text DEFAULT NULL,"
        + "KEY `blk_num` (`blk_num`)"
        + ") ENGINE=InnoDB",
        "CREATE OR REPLACE TABLE `prv_contents` ("
        + "`subnet_id` smallint(5) unsigned DEFAULT NULL,"
        + "`blk_num` bigint(20) unsigned NOT NULL,"
        + "`sig_type` tinyint(3) unsigned DEFAULT NULL,"
        + "`p2p_addr` bigint(20) unsigned DEFAULT NULL,"
        + "`bgt` bigint(20) unsigned DEFAULT NULL,"
        + "`pbh` text DEFAULT NULL,"
        + "`tx_cnt` int(11) unsigned DEFAULT NULL,"
        + "`blk_hash` text DEFAULT NULL,"
        + "`sig` text DEFAULT NULL,"
        + "PRIMARY KEY (`blk_num`)"
        + ") ENGINE=InnoDB",
        "CREATE OR REPLACE TABLE `contents` ("
        + "`subnet_id` smallint(5) unsigned DEFAULT NULL,"
        + "`db_key` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
        + "`contract` text DEFAULT NULL,"
        + "PRIMARY KEY (`db_key`)"
        + ") ENGINE=InnoDB",
        "CREATE OR REPLACE TABLE `ledgers` ("
        + "`subnet_id` smallint(5) unsigned DEFAULT NULL,"
        + "`idx` bigint(20) unsigned NOT NULL AUTO_INCREMENT,"
        + "`from_pk` text NOT NULL DEFAULT '',"
        + "`revision` int(11) unsigned NOT NULL DEFAULT 0,"
        + "`db_key` bigint(20) unsigned NOT NULL DEFAULT 0,"
        + "`blk_num` bigint(20) unsigned NOT NULL DEFAULT 0,"
        + "`to_pk` text NOT NULL DEFAULT '',"
        + "`kind` int(11) unsigned NOT NULL DEFAULT 0,"
        + "`amount` text DEFAULT NULL,"
        + "`balance` text DEFAULT NULL,"
        + "PRIMARY KEY (`to_pk`(66), `blk_num`, `db_key`, `idx`),"
        + "KEY `idx` (`idx`),"
        + "KEY `balance` (`from_pk`(66), `revision`)"
        + ") ENGINE=InnoDB",
        "CREATE OR REPLACE TABLE `info` ("
        + "`subnet_id` smallint(5) unsigned DEFAULT NULL,"
        + "`db_key` bigint(20) unsigned NOT NULL,"
        + "`blk_num` bigint(20) unsigned DEFAULT NULL,"
        + "`bgt` bigint(20) unsigned DEFAULT NULL,"
        + "`bct` bigint(20) unsigned DEFAULT NULL,"
        + "`sc_hash` text DEFAULT NULL,"
        + "PRIMARY KEY (`db_key`),"
        + "KEY `sc_hash` (`sc_hash`(64)),"
        + "KEY `block_num` (`blk_num`)"
        + ") ENGINE=InnoDB"
    ]
}

const SlaveQuerys = {
    querys: [
        "STOP SLAVE",
        "RESET SLAVE ALL",
        "SET GLOBAL server_id=",
        "CHANGE MASTER TO"
        + " MASTER_HOST=?,"
        + " MASTER_USER=?,"
        + " MASTER_PASSWORD=?,"
        + " MASTER_PORT=3306,"
        + " MASTER_LOG_FILE=?,"
        + " MASTER_LOG_POS=?,"
        + " MASTER_CONNECT_RETRY=10",
        "START SLAVE",
        "SHOW slave status"
    ]
}

const ShardClientQuerys = {
    querys: [
        "DROP USER IF EXISTS ",
        "CREATE USER ",
        "GRANT SELECT ON sc.* TO ",
        "GRANT SELECT ON block.* TO ",
        "FLUSH PRIVILEGES"
    ]
}

const TruncateDB = async (sc_conn, blk_conn) => {

    let sql = "TRUNCATE contents";
    await sc_conn.query(sql);

    sql = "TRUNCATE ledgers";
    await sc_conn.query(sql);

    sql = "TRUNCATE info";
    await sc_conn.query(sql);

    sql = "TRUNCATE contents";
    await blk_conn.query(sql);

    sql = "TRUNCATE info";
    await blk_conn.query(sql);

    sql = "TRUNCATE txs";
    await blk_conn.query(sql);

    sql = "TRUNCATE prv_contents";
    await blk_conn.query(sql);
}

module.exports.setServerID = async () => {
    if(config.NODE_RULE === define.NODE_RULE.DBN)
    {
        server_id = define.REPLICATION_SERVER_ID.DEFAULT;
    }
    else if(config.NODE_RULE === define.NODE_RULE.DN)
    {
        server_id = parseInt(config.CN_NODE_JSON.NODE.P2P.CLUSTER.ADDR.substr(config.CN_NODE_JSON.NODE.P2P.CLUSTER.ADDR.length - 4)) + 1;
    }
}

module.exports.initDatabase = async () => {
    let ret;
    let sc_conn;
    let blk_conn;
    
    const conn = await mysql.createConnection(db_config);

    try {
        let sql;

        // CREATE sc Database
        if(config.NODE_RULE === define.NODE_RULE.DN) 
        {
            sql = 'CREATE DATABASE IF NOT EXISTS `' + define.DB_NAME.DN_SC + '`';
        }
        else if(config.NODE_RULE === define.NODE_RULE.DBN)
        {
            sql = 'CREATE DATABASE IF NOT EXISTS `' + define.DB_NAME.SC + '`';
        }
        await conn.query(sql);

        // CREATE block Database

        if(config.NODE_RULE === define.NODE_RULE.DN) 
        {
            sql = 'CREATE DATABASE IF NOT EXISTS `' + define.DB_NAME.DN_BLOCK + '`';
        }
        else if(config.NODE_RULE === define.NODE_RULE.DBN)
        {
            sql = 'CREATE DATABASE IF NOT EXISTS `' + define.DB_NAME.BLOCK + '`';
        }
        await conn.query(sql);

        sc_conn = await mysql.createConnection(sc_db_config);
        blk_conn = await mysql.createConnection(blk_db_config);

        // CREATE TABLES
        await common.asyncForEach(createTables.querys, async(element, index) => {
            if(index <= define.QUERY_ARRAY_INDEX.BLOCK_DB_INDEX) {
                await blk_conn.query(element);
            } else {
                await sc_conn.query(element);
            }
        });
        
        await TruncateDB(sc_conn, blk_conn);

        sql = "STOP SLAVE";
        await sc_conn.query(sql);

        sql = "RESET SLAVE ALL";
        await sc_conn.query(sql);

        ret = { res : true };
        winlog.info("Database Init");
    } catch (err) {
        console.log(err);
        ret = { res : false, reason : JSON.stringify(err)};
    }
    await conn.end();
    await sc_conn.end();
    await blk_conn.end();

    return ret;
}

module.exports.slaveReplication = async (remoteAddr, role, fileName, position, db_key_index) => {
    let res;
    let user;
    let pwd;

    winlog.info("role : " + role);
    winlog.info("remoteAddr : " + remoteAddr);
    winlog.info("fileName : " + fileName);
    winlog.info("position : " + position);
    winlog.info("db_key_index : " + db_key_index);

    if(role === define.NODE_RULE.DN) 
    {
        user = process.env.DNUser;
        pwd = addon.AESDecryptPw(process.env.aes_seed_path, "key/me/dn_enc_pwd");

        // if rule is DN then create replication filter rewrite
        await command.ReplicationRewriteSet();
    } 
    else if(role === define.NODE_RULE.DBN)
    {
        user = process.env.DBNUser;
        pwd = addon.AESDecryptPw(process.env.aes_seed_path, "key/me/dbn_enc_pwd");

        await command.ReplicationRewriteDel();
    }

    const connection = await mysql.createConnection(sc_db_config);

    try {
        await common.asyncForEach(SlaveQuerys.querys, async (element, index) => {
            if(index === define.QUERY_ARRAY_INDEX.CHANGE_MASTER) {
                await connection.query(element, [remoteAddr, user, pwd, fileName, parseInt(position)]);
            } 
            else if(index === define.QUERY_ARRAY_INDEX.SET_SERVER_ID) {
                element += `${server_id}`;
                await connection.query(element);
            } else {
                await connection.query(element);
            }
        }); 

        // Check If Replication Success
        do { 
            [query_result] = await connection.query(SlaveQuerys.querys[define.QUERY_ARRAY_INDEX.SHOW_SLAVE_STATUS]);
            if(query_result[0].Slave_IO_Running === "Yes" && query_result[0].Slave_SQL_Running === "Yes") {
                break;
            }
            // Waitting Replication Set complete 
            common.sleep(1000);
        } while (true);   

        let sql = "ALTER TABLE contents AUTO_INCREMENT=";
        let db_key_base = db_key_index;
        db_key_base = db_key_base.slice(define.P2P_ROOT_SPLIT_INDEX.START, define.P2P_ROOT_SPLIT_INDEX.END);
        db_key_base = await common.Padding(db_key_base, define.HEX_KEY_INDEX_LEN, define.PADDING_DELIMITER.FRONT);
        db_key_base = await common.Padding(db_key_base, define.HEX_DB_KEY_LEN, define.PADDING_DELIMITER.BACK)
        db_key_base = BigInt(db_key_base);
        
        sql += `${db_key_base}`;
        await connection.query(sql);

        res = { res : true }
    } catch (err) {
        console.log(err);
        res = { res: false, reason : JSON.stringify(err)};
    }

    await connection.end();

    return res;
}

module.exports.createClient = async (user, pwd, purpose) => {
    let ret;

    if(config.NODE_RULE === define.NODE_RULE.DBN) 
    {
        let sqls;
        const connection = await mysql.createConnection(sc_db_config);
    
        if(purpose === define.DB_USER_PURPOSE.SHARD_PURPOSE) {
            sqls = ShardClientQuerys.querys;
        } 
    
        try {
            await common.asyncForEach(sqls, async (element, index) => {
                if(index === define.QUERY_ARRAY_INDEX.DROP_USER_INDEX) {
                    element += `'${user}'@'%'`;
                } else if(index === define.QUERY_ARRAY_INDEX.CREATE_USER_INDEX) {
                    element += `'${user}'@'%' IDENTIFIED BY '${pwd}'`;
                } else if(index === define.QUERY_ARRAY_INDEX.GRANT_SC_DB || index === define.QUERY_ARRAY_INDEX.GRANT_BLK_DB) {
                    element += `'${user}'@'%' WITH GRANT OPTION`;
                }
    
                [query_result] = await connection.query(element);
            });
        
            ret = { res : true };
        } catch (err) {
            ret = { res : false, reason : JSON.stringify(err)};
        }
        await connection.end();
    }
    else 
    {
        ret = { res : false, reason : "Node Rule is not DBN" };
    }
    return ret;
}

module.exports.slaveReset = async () => {
    let ret;

    const connection = await mysql.createConnection(sc_db_config);

    try {
        let sql = "STOP SLAVE";
        await connection.query(sql);

        sql = "RESET SLAVE ALL";
        await connection.query(sql);

        ret = { res : true };
        winlog.info("Replication Reset");
    } catch (err) {
        ret = { res : false, reason : JSON.stringify(err) };
    }

    await connection.end();
    return ret;

}