### Author: <gianfranco@mongodb.com>

global mongodb_user
mongodb_user = os.getenv('DSTAT_MONGODB_USER') or os.getenv('USER')

global mongodb_pwd
mongodb_pwd = os.getenv('DSTAT_MONGODB_PWD')

class dstat_plugin(dstat):
    """
    Plugin for MongoDB commands.
    """
    def __init__(self):
        self.name = 'mongodb commands'
        self.nick = ('qry', 'ins', 'upd', 'del', 'gtm', 'cmd')
        self.vars = ('query', 'insert','update','delete','getmore','command')
        self.type = 'd'
        self.width = 5
        self.scale = 2

    def check(self): 
        global pymongo
        import pymongo
        try:
            self.m = pymongo.MongoClient()
            if mongodb_pwd:
                self.m.admin.authenticate(mongodb_user, mongodb_pwd)
            self.db = self.m.admin
        except Exception, e:
            raise Exception, 'Cannot interface with MongoDB server: %s' % e

    def extract(self):
        try:
            line = self.db.command("serverStatus")['opcounters']

            for name in self.vars:
                if name in line.iterkeys():
                    if name + 'raw' in self.set2:
                        self.set2[name] = long(line.get(name)) - self.set2[name + 'raw']
                    if name == "command":
                        self.set2[name + 'raw'] = long(line.get(name)+1)
                    else:
                        self.set2[name + 'raw'] = long(line.get(name))

            for name in self.vars:
                self.val[name] = self.set2[name] * 1.0 / elapsed

            if step == op.delay:
                self.set1.update(self.set2)

        except Exception:
            for name in self.vars:
                self.val[name] = -1
