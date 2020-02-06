const execSync = require('child_process').execSync;

module.exports.ReplicationRewriteSet = () => {
    const stdout = execSync('sh run/repl_opt.sh add');
    console.log(stdout);
}

module.exports.ReplicationRewriteDel = () => {
    const stdout = execSync('sh run/repl_opt.sh remove');
    console.log(stdout);
}