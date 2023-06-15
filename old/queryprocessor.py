# This is a subclass of Processor, it serves the purpose of returning a
# dataframe containing 

from processor import Processor
from sparql_dataframe import get

class QueryProcessor(Processor):
    def __init__(self):
        super(Processor).__init__()
        self.DbPathorUrl = Processor.setDbPathorUrl
    
    def getEntitybyId(self, id):

        def getfromGraph(id):
            endpoint = self.DbPathorUrl

            query = "PREFIX sysu:<https://github.com/falaimo99/syntactic_sugars/vocabulary/> select ?e where {%s sysu:id ?e}"%(id)

            df_sparql = get(endpoint, query, True)
            
            return df_sparql
        
        def getfromTabular():
            #insert code for tabular data
            ###
            pass

        return getfromGraph(id)
        return getfromTabular()
    
