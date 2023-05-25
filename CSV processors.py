from sqlite3 import connect
from pandas import read_csv, Series, DataFrame
import pandas as pd
pd.options.mode.chained_assignment = None

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
        creators = metadata[["creator"]]
        for i in creators["creator"]:
            if ";" in i:
                creators["creator"] = creators["creator"].str.split(";") 
                creators = creators.explode("creator")
                creators = creators.reset_index(drop=True)
        values_df2 = creators["creator"].values
        list_1 =[]
        creator_internal_id = []
        for z in values_df2:
                if z not in list_1:
                        list_1.append(z)
                        creator_internal_id.append("creator-" + str(list_1.index(z)))
        else:
            for i in list_1:
                if z == i:
                    creator_internal_id.append("creator-" + str(list_1.index(i)))
        creators.insert(0,"creatorId", Series(creator_internal_id, dtype= "string")) 
        
        metadata = path1[["id", "title","creator"]]
        for index, row in metadata.iterrows():
                for item_idx, item in row.items():
                        if item_idx =="creator":
                                if ";" in item:
                                        row_to_copy = metadata.loc[index:index]  
                                        metadata = pd.concat([metadata.loc[:index], row_to_copy, metadata.loc[index+1:]]).reset_index(drop=True)
        metadata = metadata.drop(["creator"], axis = 1)          
        metadata = metadata.join(creators["creatorId"])  
                                
        metadata_id = []
        for idx, row in metadata.iterrows():
            metadata_id.append("metadata-" + str(idx))
        metadata.insert(0, "metadata_internal_id", Series(metadata_id, dtype="string"))

        with connect(self.getDbPathOrUrl()) as con:   
            metadata.to_sql("metadataId", con, if_exists="replace", index=False)
            creators.to_sql("creatorsId", con, if_exists="replace", index=False) 
        

         
        