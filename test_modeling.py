# this is just a test but it may work

class IdentifiableEntity:
    def __init__(self, identifier):
        self.id = identifier

    def getIdentifier(self):
        return self.id


class Annotation(IdentifiableEntity):
    def __init__(self, identifier, body, target, motivation):
        self.motivation = motivation
        self.target = target
        self.body = body

        super().__init__(identifier)


class Image(IdentifiableEntity):
    pass

class EntityWithMetadata(IdentifiableEntity):
    def __init__(self, identifier, label, title, creators):
        self.label = label
        self.title = title
        self.creators = creators

        super().__init__(identifier)


class Canvas(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators):
        super().__init__(identifier, label, title, creators)

class Manifest(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators, items):
        self.items = items

        super().__init__(identifier, label, title, creators)

class Collection(EntityWithMetadata):
    def __init__(self, identifier, label, title, creators, items):
        self.items = items

        super().__init__(identifier, label, title, creators)
