from queryprocessor import QueryProcessor
from sparql_dataframe import get

class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()
        self.DbPathorUrl = QueryProcessor.setDbPathorUrl

    def getAllCanvases(self):

        endpoint = self.DbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        
        SELECT ?id
        WHERE {
            ?id rdf:type sysu:Canvas .
            }

        """

        df_canvases = get(endpoint, query, True)
        
        return df_canvases
    
    def getAllCollections(self):

        endpoint = self.DbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        
        SELECT ?id
        WHERE {
            ?id rdf:type sysu:Collection .
        }

        """

        df_collections = get(endpoint, query, True)
    
        return df_collections

    def getAllManifests(self):

        endpoint = self.DbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
       
        SELECT ?id
        WHERE {
            ?id rdf:type sysu:Manifest .
        }

        """

        df_manifests = get(endpoint, query, True)
        
        return df_manifests
    
    def getCanvasesInCollection(self, collectionid: str):

        endpoint = self.DbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?man_id ?can_id 
        WHERE {
            ?id a <%s> ;
                rdf:type sysu:Collection ;
                sysu:items ?man_id.
            ?man_id rdf:type sysu:Manifest ; 
                sysu:items ?can_id .
        }

        """%(str(collectionid))

        df_canvasesincollection = get(endpoint, query, True)
        
        return df_canvasesincollection
    
    def getCanvasesInManifest(self, manifestid: str):

        endpoint = self.DbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

       SELECT ?can_id 
        WHERE {
            ?id a <%s> ;
                rdf:type sysu:Manifest ;
                sysu:items ?can_id.
        }

        """%(str(manifestid))

        df_canvasesinmanifest = get(endpoint, query, True)
        
        return df_canvasesinmanifest
    
    def getEntitiesWithLabel(self, label:str):
        
        endpoint = self.DbPathorUrl

        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id ?items ?type 
        WHERE {
            ?label a <%s> . 
            ?id sysu:label ?label ;
                rdf:type ?type .
            OPTIONAL {?id sysu:items ?items         
            } 

        }
        """%(str(label))
        
        df_entitieswithlabel = get(endpoint, query, True)
        
        return df_entitieswithlabel
    
    def getManifestsInCollection(self, collectionid: str):

        endpoint = self.DbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?man_id 
        WHERE {
            ?id a <%s>;
                rdf:type sysu:Collection ;
                sysu:items ?man_id.

        }
        """%(str(collectionid))

        df_manifestsincollection = get(endpoint, query, True)
        
        return df_manifestsincollection
    
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_qp = TriplestoreQueryProcessor()
grp_qp.setDbPathorUrl(grp_endpoint)
