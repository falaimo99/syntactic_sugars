#The processor handles the endpoint to upload the graph
#and the path to handle the final upload of a tabular database

class Processor(object): 
    def __init__(self):
        self.DbPathOrUrl = ""
        
    def setDbPathOrUrl(self, new_path): # it needs to return a boolean
        self.DbPathOrUrl = new_path
        if new_path:
            return True 
        else: 
            return False
        
        
    def getDbPathOrUrl(self):
        return self.DbPathOrUrl