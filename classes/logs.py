from datetime import datetime

class Logs:
    def __init__(self):
        pass

    def printError (db, exception):
        now = datetime.now()
        print ( "[%s][ERROR] %s" % (now.strftime("%Y-%m-%d %H:%M:%S"), exception.strip().replace("ERROR:  ", "")) )
        
    def printDebug (db, exception):
        now = datetime.now()
        print ( "[%s][INFO] %s" % (now.strftime("%Y-%m-%d %H:%M:%S"), exception.strip().replace("ERROR:  ", "")) )