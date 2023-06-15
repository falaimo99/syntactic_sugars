from QueryProcessor import QueryProcessor
from sparql_dataframe import get

class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllCanvases(self):

        endpoint = self.DbPathOrUrl
    
        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        select ?e 
        where {
        
            ?e rdf:type sysu:Canvas
            
            }

        """

        df_canvases = get(endpoint, query, True)
        
        return df_canvases
    
    def getAllCollections(self):

        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        select ?e 
        where {
        
            ?e rdf:type sysu:Collection
            
            }

        """

        df_collections = get(endpoint, query, True)
    
        return df_collections

    def getAllManifests(self):

        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        select ?e 
        where {
        
            ?e rdf:type sysu:Manifest
            
            }

        """

        df_manifests = get(endpoint, query, True)
        
        return df_manifests
    
    def getCanvasesInCollection(self, collection):

        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        select ?e
        where { 
  
        <%s> rdf:type sysu:Collection .
        ?s sysu:items ?o .
        ?o sysu:items ?e .
  
        } 

        """%(str(collection))

        df_canvasesincollection = get(endpoint, query, True)
        
        return df_canvasesincollection
    
    def getCanvasesInManifest(self, manifest):

        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        select ?e
        where { 
  
        <%s> rdf:type sysu:Manifest .
        ?s sysu:items ?o .
        ?o sysu:items ?e .
  
        } 

        """%(str(manifest))

        df_canvasesinmanifest = get(endpoint, query, True)
        
        return df_canvasesinmanifest
    
    def getManifestsInCollection(self, collection):

        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        select ?e
        where {
  
        <%s> sysu:items ?e .
        ?e rdf:type sysu:Manifest
  
        }
        """%(str(collection))

        df_manifestsincollection = get(endpoint, query, True)
        
        return df_manifestsincollection
