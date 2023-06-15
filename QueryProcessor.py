from Processor import Processor
from sparql_dataframe import get
from sqlite3 import connect
import pandas as pd

class QueryProcessor(Processor):
    def __init__(self):
        super(Processor).__init__()
        self.DbPathOrUrl = Processor.setDbPathOrUrl
    
    def getEntityById(self, id):
        if self.DbPathOrUrl.endswith(".db"):
            with connect(self.DbPathOrUrl) as con:
                annotation_query = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE annotation=?"
                annotation_df = pd.read_sql(annotation_query, con, params=(id,))

            with connect(self.DbPathOrUrl) as con:
                image_query = "SELECT body FROM image WHERE body=?"
                image_df = pd.read_sql(image_query, con, params=(id,))

            if not annotation_df.empty:
                return annotation_df
            
            elif not image_df.empty:
                return image_df
            
            else:
                return pd.DataFrame()
            
        if "http://" and "blazegraph" and "sparql" in self.DbPathOrUrl:
            query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

            select ?id ?label ?int_id ?type where{
                ?id rdf:type ?type .
                ?id sysu:label ?label .
                ?id sysu:id ?int_id
                FILTER(?id=<%s>)
            }
            """%(str(id))

            sparql_df = get(self.DbPathOrUrl, query, True)

            return sparql_df   