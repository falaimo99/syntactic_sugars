class IdentifiableEntity(object):
    def __init__(self, id:str):
        self.id = id
    def getId(self):
        return self.id

class Image(IdentifiableEntity):
    def __init__ (self,id):
        super().__init__(id)

class Annotation(IdentifiableEntity): 
    def __init__(self, id, motivation, body:Image, target:IdentifiableEntity):
        self.motivation = motivation
        self.body = body
        self.target = target
        super().__init__(id)
    def getBody(self):
        return self.body
    def getMotivation(self):
        return self.motivation
    def getTarget(self):
        return self.target

class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id:str, label:str, title:str, creators:str):
        self.label = label 
        self.title = title
        self.creators = creators
        super().__init__(id)
    def getLabel(self):
        return self.label
    def getTitle(self):
        if len(self.title) == 0:
            return None
        else:
            return self.title
    def getCreators(self):
        list_creators = []
        if len(self.creators) == 0:
            return list_creators
        elif ";" in self.creators:
            list_creators = self.creators.split("; ")
            return list_creators
        else:
            list_creators.append(self.creators)
            return list_creators

        
class Canvas(EntityWithMetadata):
    def __init__(self, id, label, title, creators):
        super().__init__(id, label, title, creators)
    

class Manifest(EntityWithMetadata):
    def __init__(self, id, label, title, creators, items):
        self.items = items
        super().__init__(id, label, title, creators)

    def getItems(self):
        if isinstance(self.items, list):
            return self.items
        else:
            list_items = []
            if ";" in self.items:
                list_items = self.items.split("; ")
                return list_items
            else:
                list_items.append(self.items)
                return list_items

class Collection(EntityWithMetadata):
    def __init__(self, id, label, title, creators, items):
        self.items = items
        super().__init__(id, label, title, creators)
        
    def getItems(self):
        if isinstance(self.items, list):
            return self.items
        else:
            list_items = []
            if ";" in self.items:
                list_items = self.items.split("; ")
                return list_items
            else:
                list_items.append(self.items)
                return list_items