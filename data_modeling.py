# this is just a test but it may work

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
        return Image
    def getMotivation(self):
        return self.motivation
    def getTarget(self):
        return IdentifiableEntity

class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, id, label, title, creators:str):
        self.label = label 
        self.title = title
        super().__init__(id)
        self.creators = list()
        self.creators.append(creators)
    def getLabel(self):
        return self.label
    def getTitle(self):
        if len(self.title) == 0:
            return None
        else:
            return self.title
    def getCreators(self):
        return self.creators


class Canvas(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators):
        super().__init__(identifier, label, title, creators)

class Manifest(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators, items):
        self.items = items
        #methods are missing
        super().__init__(identifier, label, title, creators)

class Collection(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators, items):
        self.items = items
        #methods are missing
        super().__init__(identifier, label, title, creators)
