#The processor handles the endpoint to upload the graph
#and the path to handle the final upload of a tabular database

class Processor:
    def __init__(self):
        self.dbPathorUrl = str()

    def getdbPathorUrl(self):
        return self.dbPathorUrl
    
    def setdbPathorUrl(self, PathorUrl):
        self.dbPathorUrl = PathorUrl
