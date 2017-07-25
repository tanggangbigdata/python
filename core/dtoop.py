from .cmd import cmd
from .conf import conf
import getpass

class dtoop:
    def dtoopimpot(self):
        pass

    def dtoopexport(self, config, table, dir, hdfs='', hive_query=''):
        c = cmd()
        cf = conf()
        cfg = cf.getConf(config)

        if getpass.getuser() == 'bigdata' :
            cfg['target_host'] = "yongche@" + cfg['target_host']

        command = "source /etc/bashrc;dtoop-export -u'" + cfg['username'] + "' -p'" + cfg['password'] + "' -h'" + cfg['host'] + "' -d'" + cfg['database'] + "' --table \"" + table + "\" --target-host \"" + cfg['target_host'] + "\" --input \"" + dir + "\""
        if hdfs != "" :
            command += " --target-dir \"" + hdfs + "\""
        if hive_query != "":
            command += " --hive-query \"" + hive_query + "\""
        print c.run(command);