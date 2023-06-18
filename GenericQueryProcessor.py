from data_modeling import *
from TripleStoreQueryProcessor import TriplestoreQueryProcessor
from RelationalQueryProcessor import RelationalQueryProcessor
import pandas as pd

pd.options.display.max_colwidth = 100

class GenericQueryProcessor():
    def __init__(self):
        self.queryProcessors = []

    def cleanQueryProcessors(self):
        self.queryProcessors = []
        if self.queryProcessors == []:
            return True

    def addQueryProcessor(self, processor):
        self.queryProcessors.append(processor)
        if processor in self.queryProcessors:
            return True

    def getAllAnnotations(self):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
                df = queryprocessor.getAllAnnotations()

                annotations = []

                for x, row in df.iterrows():
                    annotation = Annotation(id=row['annotation'], motivation=['motivation'], body=Image(row['body']), target=['target'])
                    annotations.append(annotation)
            
                return annotations
    
    def getAllCanvas(self):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                triplestore_df = queryprocessor.getAllCanvases()
            
            if isinstance(queryprocessor, RelationalQueryProcessor):
                relational_df = queryprocessor.getEntitiesCanvas()
        
        triplestore_df = triplestore_df.sort_values(by='int_id')
        
        df = triplestore_df.merge(relational_df, left_on='id', right_on='id')
        
        canvases = []
        
        for x, row in df.iterrows():
            canvas = Canvas(id=row['id'], label=row['label'], title=row['title'], creators=row['creators'])
            canvases.append(canvas)
        
        return canvases
    
    def getAllCollections(self):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                triplestore_df = queryprocessor.getAllCollections()

            if isinstance(queryprocessor, RelationalQueryProcessor):
                relational_df = queryprocessor.getEntitiesCollection()
       
        df = pd.concat([triplestore_df, relational_df], axis=1)
        df = df.fillna('')
        df = df.groupby(['collection', 'creators', 'label', 'title'])
        items = df['items']
        items = items.apply('; '.join)
        df = items.reset_index()
        
        collections = []

        for x, row in df.iterrows():
            collection = Collection(id=row['collection'], label=row['label'], title=row['title'], creators=['creators'], items=self.getManifestsInCollection(row['collection']))
            collections.append(collection)

        return collections
    
    def getAllImages(self):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
                df = queryprocessor.getAllImages()
                images = []
                for x, row in df.iterrows():
                    image = Image(id=row['body'])
                    images.append(image)
                
                return images

    def getAllManifests(self):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                triplestore_df = queryprocessor.getAllManifests()

            if isinstance(queryprocessor, RelationalQueryProcessor):
                relational_df = queryprocessor.getEntitiesManifest()

        triplestore_df = triplestore_df.groupby(["manifest", "label"])
        items = triplestore_df['items']
        items = items.apply('; '.join)
        triplestore_df = items.reset_index()
        
        df = pd.merge(triplestore_df, relational_df, left_on='manifest', right_on='id')

        manifests = []

        for x, row in df.iterrows():
            manifest = Manifest(id=row['manifest'], label=row['label'], title=row['title'], creators=['creators'], items=self.getCanvasesInManifest(row['manifest']))
            manifests.append(manifest)

        return manifests
    
    
    def getAnnotationsToCanvas(self, target):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
                df = queryprocessor.getAnnotationsWithTarget(target)

                atc = []
                for x, row in df.iterrows():
                    annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=Image(row['body']), target=self.getEntityById(row['target']))
                    atc.append(annotation)
                
                return atc
    
    def getAnnotationsToCollection(self, target):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
              df = queryprocessor.getAnnotationsWithTarget(target)

            atc = []
            for x, row in df.iterrows():
                annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=Image(row['body']), target=self.getEntityById(row['target']))
                atc.append(annotation)
            
            return atc
        
    def getAnnotationsToManifest(self, target):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
              df = queryprocessor.getAnnotationsWithTarget(target)

            atm = []
            for x, row in df.iterrows():
                annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=Image(row['body']), target=self.getEntityById(row['target']))
                atm.append(annotation)
            
            return atm
        
    def getAnnotationsWithBody(self, body):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
              df = queryprocessor.getAnnotationsWithBody(body)
            
            awb = []
            for x, row in df.iterrows():
                annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=Image(row['body']), target=row['target'])
                awb.append(annotation)
            
            return awb
    
    def getAnnotationsWithBodyAndTarget(self, body, target):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
              df = queryprocessor.getAnnotationsWithBodyAndTarget(body, target)

            awbat = []
            for x, row in df.iterrows():
                annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=Image(row['body']), target=row['target'])
                awbat.append(annotation)
            
            return awbat
    
    def getAnnotationsWithTarget(self, target):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
                df = queryprocessor.getAnnotationsWithTarget(target)
        
            awt = []
        for x, row in df.iterrows():
            annotation = Annotation(id=row['annotation'], motivation=row['motivation'], body=Image(row['body']), target=row['target'])
            awt.append(annotation)
        
        return awt
        
    def getCanvasesInCollection(self, collection):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                df = queryprocessor.getCanvasesInCollection(collection)
            
        cic = []

        for canvas in self.getAllCanvas():
            for id in df['id']:
                if id == canvas.getId():
                    cic.append(canvas)
        
        return cic
    
    def getCanvasesInManifest(self, manifest):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                df = queryprocessor.getCanvasesInManifest(manifest)

            cim = []

        for manifest in self.getAllCanvas():
            for id in df['id']:
                if id == manifest.getId():
                    cim.append(manifest)
        
        return cim
            
    def getEntityById(self, id:str):
        if 'annotation' in id:    
            for annotation in self.getAllAnnotations():
                if id == annotation.getId():
                    return annotation
        elif id.endswith('.jpg' or '.png' or '.tiff' or 'gif'):
            for image in self.getAllImages():
                if id == image.getId():
                    return image
        elif 'canvas' in id:    
            for canvas in self.getAllCanvas():
                if id == canvas.getId():
                    return canvas
        elif 'manifest' in id:
            for manifest in self.getAllManifests():
                if id == manifest.getId():
                    return manifest
        elif 'collection' in id:      
            for collection in self.getAllCollections():
                if id == collection.getId():
                    return collection
        else:
            return None
    
    def getEntitiesWithCreator(self, creator):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
                relational_df = queryprocessor.getEntitiesWithCreator(creator)

        ewc = []

        for x, row in relational_df.iterrows():
            entity_with_metadata = EntityWithMetadata(id=row['id'], label=self.getEntityById(row['id']).getLabel(), title=row['title'], creators=row['creators'])
            ewc.append(entity_with_metadata)

        return ewc

    def getEntitiesWithLabel(self, label):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                triplestore_df = queryprocessor.getEntitiesWithLabel(label)

        ewc = []

        for x, row in triplestore_df.iterrows():
                entity_with_metadata = EntityWithMetadata(id=row['id'], label=row['label'], title=self.getEntityById(row['id']).getTitle(), creators=self.getEntityById(row['id']).getCreators())
                ewc.append(entity_with_metadata)

        return ewc
    
    def getEntitiesWithTitle(self, title):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, RelationalQueryProcessor):
                relational_df = queryprocessor.getEntitiesWithTitle(title)
                
        ewt = []

        for x, row in relational_df.iterrows():
            entity_with_metadata = EntityWithMetadata(id=row['id'], label=self.getEntityById(row['id']).getLabel(), title=row['title'], creators=row['creators'])
            ewt.append(entity_with_metadata)

        return ewt
    
    def getImagesAnnotatingCanvas(self, canvas):

        iac = []

        for i in self.getAnnotationsToCanvas(canvas):
            image = i.getBody()
            iac.append(image)

        return iac
    
    def getManifestsInCollection(self, collection):
        for queryprocessor in self.queryProcessors:
            if isinstance(queryprocessor, TriplestoreQueryProcessor):
                df = queryprocessor.getManifestsInCollection(collection)
            mic = []

        for manifest in self.getAllManifests():
            for collection in df['id']:
                if collection == manifest.getId():
                    mic.append(manifest)
        
        return mic