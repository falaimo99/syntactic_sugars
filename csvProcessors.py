from sqlite3 import connect
from pandas import read_csv, Series, DataFrame
import pandas as pd
from Processor import Processor

class AnnotationProcessor(Processor): 
    def __init__(self):
        super().__init__()

    def uploadData(self, path:str):
        
        annotations = read_csv(path, keep_default_na=False, dtype={"id":"string", "body":"string", "target":"string", "motivation":"string"})
        if annotations.empty:
            raise ValueError("CSV file is empty")
        else:
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

            with connect(self.DbPathOrUrl) as con:   
                annotations_ids.to_sql("Annotation", con, if_exists="replace", index=False)
                image_ids.to_sql("Image", con, if_exists="replace", index=False) 

            if annotations_ids.empty and image_ids.empty:
                return False
            else:
                return True 


class MetadataProcessor(Processor):
    def __init__(self):
        super().__init__()
    def uploadData(self, path:str):
        path1 = read_csv(path, keep_default_na=False, dtype={"id":"string", "creator":"string", "title":"string"})
        if path1.empty:
            raise ValueError("CSV file is empty")
        else:
            metadata = path1[["id", "title","creator"]]
            metadata = metadata.rename(columns={"creator": "creators"})
            metadata_id = []
            for idx, row in metadata.iterrows():
                metadata_id.append("metadata-" + str(idx))
            metadata.insert(0, "metadata_internal_id", Series(metadata_id, dtype="string")) 
                
            creator_list = []
            int_id_list = []
            for idx, row in metadata.iterrows():
                for column_name, cell_value in row.items():
                        if column_name == 'creators':
                            if ';' in cell_value:
                                cell_value = cell_value.split("; ")
                                num_items = len(cell_value)
                                int_id_list.extend([row.iloc[0]] * num_items)
                                for i in cell_value:
                                    creator_list.append(i)
                            else:
                                if cell_value != "":
                                    creator_list.append(cell_value)
                                    first_element = row.iloc[0]
                                    int_id_list.append(first_element)
            creators = pd.DataFrame({'metadata_internal_id': int_id_list, 'creator': creator_list})

            with connect(self.DbPathOrUrl) as con:   
                metadata.to_sql("EntityWithMetadata", con, if_exists="replace", index=False)
                creators.to_sql("creators_table", con, if_exists="replace", index=False) 
                
            if metadata.empty and creators.empty:
                return False
            else:
                return True 
            