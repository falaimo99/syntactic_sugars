# This is a subclass of Processor, it serves the purpose of returning a
# dataframe containing 

from Processor import Processor
from sparql_dataframe import get

class QueryProcessor(Processor):
    def __init__(self):
        super(Processor).__init__()
        self.DbPathOrUrl = Processor.setDbPathOrUrl
    
    def getEntitybyId(self, id):

        def getfromGraph(id):
            endpoint = self.DbPathOrUrl

            query = """
                    PREFIX sysu:<https://github.com/falaimo99/syntactic_sugars/vocabulary/>
                    select ?e 
                    where {
                        ?e sysu:id <%s>
                    }
                    """%(str(id))

            df_sparql = get(endpoint, query, True)
            
            return df_sparql
        
        def getfromTabular():
            #insert code for tabular data
            ###
            pass

        return getfromGraph(id)
    