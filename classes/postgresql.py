
import subprocess

from classes.logs import Logs
logs = Logs()

class Postgresql:
    def __init__(self):
        pass
    
    def executeQuery (self, connection, query):
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as error:
            if ("None" in error.pgerror or "already exists" in error.pgerror or "already subscribes" in error.pgerror):
                logs.printDebug (error.pgerror)
            else:
                logs.printError (error.pgerror)
                exit(1)
        finally: 
            logs.printDebug( query )
    
    def executeBash (self, command):
        try:
            tableDump = subprocess.run(command, shell=True)
        except Exception as error:
            logs.printError (error)
            exit(1)
    
    def importTableSchema ():
        print("asd6")