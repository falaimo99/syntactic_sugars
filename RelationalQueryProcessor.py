from sqlite3 import connect
from pandas import read_csv, Series, DataFrame, read_sql
import pandas as pd
import unittest
from QueryProcessor import QueryProcessor
pd.options.mode.chained_assignment = None



class RelationalQueryProcessor(QueryProcessor):
    def __init__(self):
        super().__init__()

    def getAllAnnotations(self):
        with connect(self.DbPathOrUrl) as con: 
            query_1 = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId"
            all_annotations_query = read_sql(query_1,con)
        return all_annotations_query

    def getAllImages(self):
        with connect(self.DbPathOrUrl) as con: 
            query_2 = "SELECT body FROM image"
            all_images_query = read_sql(query_2,con)
        return all_images_query

    def getAnnotationsWithBody(self, bodyId): 
        with connect(self.DbPathOrUrl) as con: 
            query_3 = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE body=?"
            annotations_with_body_query = read_sql(query_3,con,params=(bodyId,))
        return annotations_with_body_query

    def getAnnotationsWithBodyAndTarget(self, bodyId, targetId): 
        with connect(self.DbPathOrUrl) as con: 
            query_4 = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE body=? AND target=?"
            annotations_with_body_and_target_query = read_sql(query_4,con,params=(bodyId, targetId))
        return annotations_with_body_and_target_query

    def getAnnotationsWithTarget(self, targetId): 
        with connect(self.DbPathOrUrl) as con: 
            query_5 = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE target=?"
            annotations_with_target_query = read_sql(query_5,con,params=(targetId,))
        return annotations_with_target_query

    def getEntitiesWithCreator(self, creator_name): 
        with connect(self.DbPathOrUrl) as con: 
            query_6 = "SELECT DISTINCT id,title,creators FROM EntityWithMetadata LEFT JOIN creators_table ON EntityWithMetadata.creatorId == creators_table.creatorId WHERE creator=?"
            entities_with_creator_query = read_sql(query_6,con,params=(creator_name,))
        return entities_with_creator_query

    def getEntitiesWithTitle(self, title): 
        with connect(self.DbPathOrUrl) as con: 
            query_7 = "SELECT  DISTINCT * FROM EntityWithMetadata WHERE title=?"
            entities_with_title_query = read_sql(query_7,con,params=(title,))
        return entities_with_title_query

relational_p = RelationalQueryProcessor()
relational_p.setDbPathOrUrl("relational.db")
print(relational_p.getAllAnnotations())