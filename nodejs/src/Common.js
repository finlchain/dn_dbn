const define = require('./../conf/define.js');
const fs = require('fs');

module.exports.stringSplit = (text, separator, limit) => {
    let splitArray;
    if(limit != null) {
        text = text.split(separator, limit);     
    } else {
        text = text.split(separator);
    }
    splitArray = [...text];
    return splitArray;
}

module.exports.asyncForEach = async (array, callback) => {
    for(let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}

module.exports.stringReplace = (str, searchStr, replaceStr) => {
    return str.split(searchStr).join(replaceStr);
}

module.exports.checkIP = (ipAddr) => {
    if(define.REGEX.IP_ADDR_REGEX.test(ipAddr)) {
        return true;
    }
    return false;
}

module.exports.sleep = (ms) => {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
}

module.exports.Padding = (data, len, separator) => {
    if(separator == define.PADDING_DELIMITER.FRONT) {
        while(data.length < len) {
            data = "0" + data;
            if(data.length == len) break;
            else continue;
        }
    }
    else if(separator == define.PADDING_DELIMITER.BACK) {
        while(data.length < len) {
            data = data + "0";
            if(data.length == len) break;
            else continue;
        }
    }
    return data;
}

module.exports.isIntegerValue = (strNum) => {
    return Number.isInteger(parseInt(strNum));
}

module.exports.isArray = (arr) => {
    return (!!arr) && (arr.constructor === Array);
}

module.exports.isObject = (obj) => {
    return (!!obj) && (obj.constructor === Object);
}

module.exports.isQueryResultObject = (variable) => {
    return variable === Object(variable);
}

module.exports.isJsonString = (str) => {
    try {
        JSON.parse(str);
    } catch (e) {
        return false;
    }
    return true;
}

module.exports.isFileExist = (filePath) => {
    return fs.existsSync(filePath)
}