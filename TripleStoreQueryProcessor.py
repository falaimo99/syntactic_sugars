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
        select ?id ?label 
        where {
        
            ?id rdf:type sysu:Canvas .
            ?id sysu:label ?label
            
            }

        """

        df_canvases = get(endpoint, query, True)
        
        return df_canvases
    
    def getAllCollections(self):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        select ?collection ?label ?items where {
            ?collection rdf:type sysu:Collection .
            ?collection sysu:label ?label .
            ?collection sysu:items ?items
        } 
        """

        df_collections = get(endpoint, query, True)
    
        return df_collections

    def getAllManifests(self):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        select ?manifest ?label ?items
        where {
        
            ?manifest rdf:type sysu:Manifest .
            ?manifest sysu:label ?label .
            ?manifest sysu:items ?items
            
            }

        """

        df_manifests = get(endpoint, query, True)
        
        return df_manifests
    
    def getCanvasesInCollection(self, collection):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        select ?id
        where { 
  
        <%s> sysu:items ?o .
        ?o sysu:items ?id .
        ?id sysu:label ?label
  
        } 

        """%(str(collection))

        df_canvasesincollection = get(endpoint, query, True)
        
        return df_canvasesincollection
    
    def getCanvasesInManifest(self, manifest):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        select ?id
        where { 
  
        <%s> sysu:items ?id .
  
        } 

        """%(str(manifest))

        df_canvasesinmanifest = get(endpoint, query, True)
        
        return df_canvasesinmanifest
    
    def getManifestsInCollection(self, collection):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        select ?id
        where {
  
        <%s> sysu:items ?id .
        ?id rdf:type sysu:Manifest
  
        }
        """%(str(collection))

        df_manifestsincollection = get(endpoint, query, True)
        
        return df_manifestsincollection
    
    def getEntitiesWithLabel(self, label):

        endpoint = self.DbPathOrUrl

        query_2 ="""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        select ?id ?label ?type where{
            ?id sysu:label ?label .
          	?id rdf:type ?type .
            FILTER(?label="%s")
        }
        """%(str(label))

        return get(endpoint,query_2,True)
    
tqp =TriplestoreQueryProcessor()
tqp.setDbPathOrUrl('http://127.0.0.1:9999/blazegraph/sparql')
print(tqp.getEntitiesWithLabel("BO0451_CAM6537_0002_contropiatto anteriore.jpg"))