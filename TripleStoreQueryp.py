from processor import Processor
from queryprocessor import QueryProcessor
from SPARQLWrapper import SPARQLWrapper
from pandas import DataFrame

class TriplestoreQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def query_block(self, endpoint, query):
        sparql_df = SPARQLWrapper(endpoint)
        sparql_df.setQuery(query)
        results = sparql_df.query()
        return results

    def getAllCanvases(self) -> DataFrame:  
        endpoint = Processor.getdbPathorUrl
        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id
        WHERE {
            ?id rdf:type sysu:Canvas .
        }
        
        """
        result= self.query_block(endpoint, query)
        return result
    
    def getAllCollections(self) -> DataFrame:
        endpoint = Processor.getdbPathorUrl
        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id
        WHERE {
            ?id rdf:type sysu:Collection .
        }
        
        """
        result= self.query_block(endpoint, query)
        return result      

    def getAllManifests(self) -> DataFrame:
        endpoint = Processor.getdbPathorUrl
        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id
        WHERE {
            ?id rdf:type sysu:Manifest .
        }
        
        """
        result= self.query_block(endpoint, query)
        return result

    def getCanvasesInCollection(self, collectionid: str) -> DataFrame:
        endpoint = Processor.getdbPathorUrl
        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?man_id ?can_id 
        WHERE {
            ?id a '"""+str(collectionid)+"""' ;
                rdf:type sysu:Collection ;
                sysu:items ?man_id.
            ?man_id rdf:type sysu:Manifest ; 
                sysu:items ?can_id .
        }
        """
        result= self.query_block(endpoint, query)
        return result

    def getCanvasesInManifest(self, manifestid: str) -> DataFrame:
        endpoint = Processor.getdbPathorUrl
        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?can_id 
        WHERE {
            ?id a '"""+str(manifestid)+"""' ;
                rdf:type sysu:Manifest ;
                sysu:items ?can_id.
        }
        """
        
        result= self.query_block(endpoint, query)
        return result
    
    def getEntitiesWithLabel(self, label:str) -> DataFrame:
        endpoint = Processor.getdbPathorUrl
        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id ?items ?type 
        WHERE {
            ?label a '"""+str(label)+"""' . 
            ?id sysu:label ?label ;
                rdf:type ?type .
            OPTIONAL {?id sysu:items ?items         
            } 

        }
        """
        result= self.query_block(endpoint, query)
        return result

    def getManifestsInCollection(self, collectionid: str) -> DataFrame:
        endpoint = Processor.getdbPathorUrl
        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?man_id 
        WHERE {
            ?id a '"""+str(collectionid)+"""'
                rdf:type sysu:Collection ;
                sysu:items ?man_id.

        }
        """
        result= self.query_block(endpoint, query)
        return result
    
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_qp = TriplestoreQueryProcessor()
grp_qp.setdbPathorUrl(grp_endpoint)
