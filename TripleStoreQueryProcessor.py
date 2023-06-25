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
        
        SELECT ?id ?label ?int_id
        
        WHERE {
            ?id rdf:type sysu:Canvas ;
                sysu:label ?label ;
                sysu:id ?int_id .
            
            }
        """

        df_canvases = get(endpoint, query, True)
        
        return df_canvases
    
    def getAllCollections(self):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?collection ?label ?items 
        
        WHERE {
            ?collection rdf:type sysu:Collection ;
                        sysu:label ?label ;
                        sysu:items ?items.
        } 
        """

        df_collections = get(endpoint, query, True)
    
        return df_collections

    def getAllManifests(self):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        
        SELECT ?manifest ?label ?items
        WHERE {
            ?manifest rdf:type sysu:Manifest ;
                      sysu:label ?label ;
                      sysu:items ?items .
        }
        """

        df_manifests = get(endpoint, query, True)
        
        return df_manifests
    
    def getCanvasesInCollection(self, collection):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id
        WHERE { 
            <%s> sysu:items ?man_id .
            ?man_id sysu:items ?id .
  
        } 

        """%(str(collection))

        df_canvasesincollection = get(endpoint, query, True)
        
        return df_canvasesincollection
    
    def getCanvasesInManifest(self, manifest):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id
        WHERE { 
  
        <%s> rdf:type sysu:Manifest ;
            sysu:items ?id .
  
        } 

        """%(str(manifest))

        df_canvasesinmanifest = get(endpoint, query, True)
        
        return df_canvasesinmanifest
    
    def getEntitiesWithLabel(self, label):

        endpoint = self.DbPathOrUrl

        query ="""
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        
        SELECT ?id ?label ?type 
        
        WHERE {
            "%s" rdf:type ?label .
            ?id sysu:label ?label ;
          	    rdf:type ?type .
    
        }
        """%(str(label))

        df_entitieswithlabel = get(endpoint,query,True)
        
        return df_entitieswithlabel
    
    def getManifestsInCollection(self, collection):

        endpoint = self.DbPathOrUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id
        
        WHERE {
            <%s> sysu:items ?id .
            ?id rdf:type sysu:Manifest . 
  
        }
        """%(str(collection))

        df_manifestsincollection = get(endpoint, query, True)
        
        return df_manifestsincollection
