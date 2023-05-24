from sqlite3 import connect
from pandas import read_csv, Series, DataFrame
import pandas as pd

class Processor(object): #it needs to be modified
    def __init__(self, dbPathOrUrl):
        self.dbPathOrUrl = dbPathOrUrl
        self.dbPathOrUrl = ""
        
    def setDbPathOrUrl(self, new_path):
        self.dbPathOrUrl= new_path
        if new_path:
            return True 
        else: 
            return False
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl


class AnnotationProcessor(Processor): 
    def __init__(self, dbPathOrUrl):
        super().__init__(dbPathOrUrl)

    def uploadData(self, path:str):
        
        annotations = read_csv(path, keep_default_na=False, dtype={"id":"string", "body":"string", "target":"string", "motivation":"string"})

        image_ids = annotations[["body"]]
        image_internal_id = []
        for idx, row in image_ids.iterrows():
            image_internal_id.append("image-" + str(idx))

        image_ids.insert(0, "imageId", Series(image_internal_id, dtype="string"))

        annotations_ids = annotations[["id", "target", "motivation"]]
        annotations_ids = annotations_ids.rename(columns={"id": "annotation"})
        annotations_internal_id = []
        for idx, row in annotations_ids.iterrows():
            annotations_internal_id.append("annotations-" + str(idx))

        annotations_ids.insert(0, "annotationsId", Series(annotations_internal_id, dtype="string"))
        annotations_ids = annotations_ids.join(image_ids["imageId"]) 

        with connect(self.getDbPathOrUrl()) as con:   
            annotations_ids.to_sql("annotationsId", con, if_exists="replace", index=False)
            image_ids.to_sql("imageId", con, if_exists="replace", index=False)  

class MetadataProcessor(Processor):
    def __init__(self, dbPathorUrl):
        super().__init__(dbPathorUrl)
    def uploadData(self, path:str):
        path1 = read_csv(path, keep_default_na=False, dtype={"id":"string", "creator":"string", "title":"string"})
        
        proxy = path1[["creator"]]
        proxy1 = proxy["creator"]
        list1= proxy1.tolist()
        for idx, i in enumerate(list1):
            if ";" in i:
                x = i.split(";") 
                list1.remove(i)
                list1.insert(idx,x)
        for idx, z in enumerate(list1):
            if type(z) ==list:
                for k in z:
                    list1.insert(idx,k)
                    z.remove(k)
                    if len(z) == 0:
                        list1.remove(z)
        proxy_series = Series(list1)
        proxy_dataframe = DataFrame({"creator": list1})

        proxy_id = []
        for idx, row in proxy_dataframe.iterrows():
            proxy_id.append("creator-" + str(idx))
        proxy_dataframe.insert(0, "creator_id", Series(proxy_id, dtype="string"))

        
        metadata = path1[["id", "title","creator"]]
        metadata_id = []
        for idx, row in metadata.iterrows():
                    metadata_id.append("metadata-" + str(idx))

        for index, row in metadata.iterrows():
                for item_idx, item in row.items():
                        if item_idx =="creator":
                                if ";" in item:
                                        row_to_copy = metadata.loc[index:index]  
                                        metadata = pd.concat([metadata.loc[:index], row_to_copy, metadata.loc[index+1:]]).reset_index(drop=True)

        metadata = metadata.drop(["creator"], axis = 1)          
        metadata = metadata.join(proxy_dataframe["creator_id"])   

        metadata_id = []
        for idx, row in metadata.iterrows():
            metadata_id.append("metadata-" + str(idx))
        metadata.insert(0, "metadata_internal_id", Series(metadata_id, dtype="string"))

        with connect(self.getDbPathOrUrl()) as con:   
            metadata.to_sql("metadataId", con, if_exists="replace", index=False)
            proxy_dataframe.to_sql("creatorsId", con, if_exists="replace", index=False)

         
        