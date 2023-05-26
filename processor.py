#The processor handles the endpoint to upload the graph
#and the path to handle the final upload of a tabular database

class Processor(object): 
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = dbPathOrUrl
        self.dbPathOrUrl = ""
        
    def setDbPathOrUrl(self, new_path): # it needs to return a boolean
        self.dbPathOrUrl= new_path
        if new_path:
            return True 
        else: 
            return False
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
