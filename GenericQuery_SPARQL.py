from processor import Processor
from queryprocessor import QueryProcessor
from sparql_dataframe import get
from sqlite3 import connect
from pandas import read_csv, Series, DataFrame, read_sql
import pandas as pd

# getAllCanvas: it returns a list of objects having class Canvas included in the databases accessible via the query processors.
# getAllCollections: it returns a list of objects having class Collection included in the databases accessible via the query processors.
#getAllManifests: it returns a list of objects having class Manifest included in the databases accessible via the query processors.
#getCanvasesInCollection: it returns a list of objects having class Canvas, included in the databases accessible via the query processors, that are contained in the collection identified by the input identifier.
#getCanvasesInManifest: it returns a list of objects having class Canvas, included in the databases accessible via the query processors, that are contained in the manifest identified by the input identifier.
#getEntitiesWithLabel: it returns a list of objects having class EntityWithMetadata, included in the databases accessible via the query processors, related to the entities having, as label, the input label.
#getManifestsInCollection: it returns a list of objects having class Manifest, included in the databases accessible via the query processors, that are contained in the collection identified by the input identifier.

class GenericQueryProcessor(QueryProcessor):

    def __init__(self):
        super().__init__()
        self.dbPathorUrl = QueryProcessor.setdbPathorUrl



    def getAllCanvas(self):

        endpoint = self.dbPathorUrl

        query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        
            SELECT ?id ?label
            WHERE {
                ?id rdf:type sysu:Canvas .
                ?id sysu:label ?label . 
                }
                """
        df_canvas = get(endpoint, query, True)
        
        return df_canvas
    
    def getAllCollections(self):

        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
        
        SELECT ?id ?label ?items
        WHERE {
            ?id rdf:type sysu:Collection .
            ?id sysu:label ?label .
            ?id sysu:items ?items. 
        }

        """
        df_collections = get(endpoint, query, True)
    
        return df_collections
    
    def getAllManifests(self):
        
        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>
       
        SELECT ?id ?label ?items
        WHERE {
            ?id rdf:type sysu:Manifest .
            ?id sysu:label ?label . 
            ?id sysu:items ?items .
        }

        """

        df_manifests = get(endpoint, query, True)
        
        return df_manifests

    def getCanvasesInCollection(self):

        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?man_id ?can_id ?label
        WHERE {
            ?id rdf:type sysu:Collection ;
                sysu:items ?man_id.
            ?man_id rdf:type sysu:Manifest ; 
                sysu:items ?can_id .
            ?can_id sysu:label ?label .
            FILTER(?id=<%s>).
        }

        """%(str(collectionid))

        df_canvasesincollection = get(endpoint, query, True)
        
        return df_canvasesincollection
    
    def getCanvasesInManifest(self, manifestid: str):
        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

       SELECT ?can_id 
        WHERE {
            ?id rdf:type sysu:Manifest ;
                sysu:items ?can_id.
            ?can_id sysu:label ?label .
            FILTER(?id<%s>).
        }

        """%(str(manifestid))

        df_canvasesinmanifest = get(endpoint, query, True)
        
        return df_canvasesinmanifest
    def getEntitiesWithLabel(self, label:str):
        
        endpoint = self.dbPathorUrl

        query = """
        
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu: <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?id ?type 
        WHERE {
            ?id sysu:label ?label ;
                rdf:type ?type . 
            FILTER(?label="%s")
        }
        """%(str(label))
        
        df_entitieswithlabel = get(endpoint, query, True)
        
        return df_entitieswithlabel
    
    def getManifestsInCollection(self, collectionid: str):

        endpoint = self.dbPathorUrl

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

        SELECT ?man_id ?label ?items
        WHERE {
            ?id rdf:type sysu:Collection ;
                sysu:items ?man_id.
            ?man_id sysu:label ?label ;
                    sysu:items ?items .
            FILTER(?id=<%s>)

        }
        """%(str(collectionid))

        df_manifestsincollection = get(endpoint, query, True)
        
        return df_manifestsincollection

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_qp = GenericQueryProcessor()
grp_qp.setdbPathorUrl(grp_endpoint)