class Processor(object): 
    def __init__(self):
        self.DbPathOrUrl = ""
        
    def setDbPathOrUrl(self, new_path):
        self.DbPathOrUrl = new_path
        if new_path:
            return True 
        else: 
            return False
        
        
    def getDbPathOrUrl(self):
        return self.DbPathOrUrl