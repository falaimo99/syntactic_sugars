from sqlite3 import connect
from pandas import read_csv, Series, DataFrame, read_sql
import pandas as pd
import unittest
pd.options.mode.chained_assignment = None


class Processor:
    def __init__(self, dbPathOrUrl=None):
        self.dbPathOrUrl = dbPathOrUrl

    def getdbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setdbPathOrUrl(self, PathOrUrl):
        self.dbPathOrUrl = PathOrUrl
        if PathOrUrl == None:
            return False
        else:
            return True 

class RelationalQueryProcessor(Processor):
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)
        self.dbPathOrUrl = self.setdbPathOrUrl(dbPathOrUrl)

    def getAllAnnotations(self):
        with connect(self.getdbPathOrUrl()) as con: 
            query_1 = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId"
            all_annotations_query = read_sql(query_1,con)
        return all_annotations_query
    
    def getAllImages(self):
        with connect(self.getdbPathOrUrl()) as con: 
            query_2 = "SELECT body FROM image"
            all_images_query = read_sql(query_2,con)
        return all_images_query

    def getAnnotationsWithBody(self, bodyId): 
        with connect(self.getdbPathOrUrl()) as con: 
            query_3 = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE body=?"
            annotations_with_body_query = read_sql(query_3,con,params=(bodyId,))
        return annotations_with_body_query

    def getAnnotationsWithBodyAndTarget(self, bodyId, targetId): 
        with connect(self.getdbPathOrUrl()) as con: 
            query_4 = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE body=? AND target=?"
            annotations_with_body_and_target_query = read_sql(query_4,con,params=(bodyId, targetId))
        return annotations_with_body_and_target_query

    def getAnnotationsWithTarget(self, targetId): 
        with connect(self.getdbPathOrUrl()) as con: 
            query_5 = "SELECT annotation,target,body,motivation FROM Annotation LEFT JOIN image ON Annotation.imageId == image.imageId WHERE target=?"
            annotations_with_target_query = read_sql(query_5,con,params=(targetId,))
        return annotations_with_target_query

    def getEntitiesWithCreator(self, creator_name): 
        with connect(self.getdbPathOrUrl()) as con: 
            query_6 = "SELECT DISTINCT id,title,creators FROM EntityWithMetadata LEFT JOIN creators_table ON EntityWithMetadata.creatorId == creators_table.creatorId WHERE creator=?"
            entities_with_creator_query = read_sql(query_6,con,params=(creator_name,))
        return entities_with_creator_query

    def getEntitiesWithTitle(self, title): 
        with connect(self.getdbPathOrUrl()) as con: 
            query_7 = "SELECT id,title,creators FROM EntityWithMetadata LEFT JOIN creators_table ON EntityWithMetadata.creatorId == creators_table.creatorId WHERE title=?"
            entities_with_title_query = read_sql(query_7,con,params=(title,))
        return entities_with_title_query
    
class TestRelationalQueryProcessor(unittest.TestCase):
    def test_04_RelationalQueryProcessor(self):
        rel_qp = RelationalQueryProcessor('relational.db')
        db_path_or_url = 'relational.db'
        self.assertTrue(rel_qp.setdbPathOrUrl(db_path_or_url))

        self.assertIsInstance(rel_qp.getAllAnnotations(), DataFrame)
        self.assertIsInstance(rel_qp.getAllImages(), DataFrame)
        self.assertIsInstance(rel_qp.getAnnotationsWithBody("just_a_test"), DataFrame)
        self.assertIsInstance(rel_qp.getAnnotationsWithBodyAndTarget("just_a_test", "another_test"), DataFrame)
        self.assertIsInstance(rel_qp.getAnnotationsWithTarget("just_a_test"), DataFrame)
        self.assertIsInstance(rel_qp.getEntitiesWithCreator("just_a_test"), DataFrame)
        self.assertIsInstance(rel_qp.getEntitiesWithTitle("just_a_test"), DataFrame)
if __name__ == '__main__':
    unittest.main()
