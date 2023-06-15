# %% [markdown]
# ### GenericQueryProcessor
# This is the final class for the project, a class that handles instances of the two children class of `QueryProcessor`.
# It is an indipendent class, the combination through `concat` of `Pandas` is possible with a call to the attribute `self.queryProcessors`, a list where a `QueryProcessor` is stored, after its instantiation.
# The methods call on single class method or combine them and extract from the dataframe the selected

# %%
## This is the first attempt in order to understand how to make the final product work

#It is needed to import the two query_sparql processors class

from data_modeling import *
from TripleStoreQueryProcessor import TriplestoreQueryProcessor
from RelationalQueryProcessor import RelationalQueryProcessor
from pandas import read_csv, Series, DataFrame, read_sql
from sparql_dataframe import get
from sqlite3 import connect
import pandas as pd
import tabloo



class GenericQueryProcessor():
    # The first thing it does is initialize the two query_sparql processors needed
    def __init__(self):
        self.queryProcessors = []

    def cleanQueryProcessors(self):
        self.queryProcessors = []
        if not self.queryProcessors:
            return True
            #the guidelines ask for a boolean, not sure on how to interpret that

    def addQueryProcessors(self, processor):
        self.queryProcessors.append(processor)
        if processor in self.queryProcessors:
            return True
            #the guidelines ask for a boolean, not sure on how to interpret that

    def getAllAnnotations(self):    
        for queryprocessor in self.queryProcessors:
            if queryprocessor is RelationalQueryProcessor():    
                df = queryprocessor.getAllAnnotations()
                annotations = []
                for x, row in df.iterrows():
                    annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=row['body'], target=row['target'])
                    annotations.append(annotation)
                
                return annotations
    
    def getallCanvas(self):
        for queryprocessor in self.queryProcessors:

            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                query_sparql = """
                PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX sysu:  <https://github.com/falaimo99/syntactic_sugars/vocabulary/>

                select ?canvas ?label where {
                    ?canvas rdf:type sysu:Canvas .
                    ?canvas sysu:label ?label
                } 
                """
                label_df = get(queryprocessor.DbPathOrUrl, query_sparql, True)
            
            if isinstance(queryprocessor, RelationalQueryProcessor):
                with connect(queryprocessor.DbPathOrUrl) as con:
                    query_sql = "SELECT DISTINCT id,creators,title FROM EntityWithMetadata WHERE id LIKE '%canvas%'"
                    canvas_df = read_sql(query_sql, con)
            
        df = pd.concat([canvas_df, label_df], axis=1)
        pd.set_option("display.expand_frame_repr", False)
        print(df)

    def getAllImages(self):    
        for queryprocessor in self.queryProcessors:
            if queryprocessor is RelationalQueryProcessor():    
                df = queryprocessor.getAllImages()
                images = []
                for x, row in df.iterrows():
                    image = Image(id=row['body'])
                    images.append(image)
                
                return images
            
    def getAnnotationsToCanvas(self, target):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):   
                with connect(queryprocessor.DbPathOrUrl) as con:
                    query = "SELECT * FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE target=?"
                    df = read_sql(query, con, params=(target,))
                atc = []
                for x, row in df.iterrows():
                    annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=row['body'], target=row['target'])
                    atc.append(annotation)
                
                return atc
    
    def getAnnotationsWithBody(self):    
        for queryprocessor in self.queryProcessors:
            if queryprocessor is RelationalQueryProcessor():    
                df = queryprocessor.getAnnotationsWithBody()
                annotations_body = []
                for x, row in df.iterrows():
                    annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=row['body'], target=row['target'])
                    annotations_body.append(annotation)
                
                return annotations_body
    
    def getAnnotationsWithBodyandTarget(self):    
        for queryprocessor in self.queryProcessors:
            if queryprocessor is RelationalQueryProcessor():    
                df = queryprocessor.getAnnotationsWithBodyandTarget()
                annotations_bt = []
                for x, row in df.iterrows():
                    annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=row['body'], target=row['target'])
                    annotations_bt.append(annotation)
                
                return annotations_bt
        
    def getAnnotationsWithTarget(self):    
        for queryprocessor in self.queryProcessors:
            if queryprocessor is RelationalQueryProcessor():    
                df = queryprocessor.getAnnotationsWithTarget()
                annotations_t = []
                for x, row in df.iterrows():
                    annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=row['body'], target=row['target'])
                    annotations_t.append(annotation)
                
                return annotations_t
            
    def getEntitiesWithCreator(self):    #manca il label che è da ricevere attraverso sparql query
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                pass
            if isinstance(queryprocessor, RelationalQueryProcessor):  
                df = queryprocessor.getEntitiesWithCreator()
                ewc = []
                for x, row in df.iterrows():
                    entity = EntityWithMetadata(id=row['id'], label=row['label'], title=row['title'], creators=row['creators'])
                    ewc.append(entity)
                
                return ewc
            
    def getImagesAnnotatingCanvas(self, target):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):   
                with connect(queryprocessor.DbPathOrUrl) as con:
                    query = "SELECT body FROM  Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE target=?"
                    df = read_sql(query, con, params=(target,))
                img_a = []
                for x, row in df.iterrows():
                    image = Image(id=row['body'])
                    img_a.append(image)
                
                return img_a
        



# %%
r_qp = RelationalQueryProcessor()
r_qp.setDbPathOrUrl("relational.db")

t_qp = TriplestoreQueryProcessor()
t_qp.setDbPathOrUrl("http://127.0.0.1:9999/blazegraph/sparql")

g_qp = GenericQueryProcessor()
g_qp.addQueryProcessors(r_qp)
g_qp.addQueryProcessors(t_qp)

g_qp.getAnnotationsToCanvas('https://dl.ficlit.unibo.it/iiif/2/28429/canvas/p8')

