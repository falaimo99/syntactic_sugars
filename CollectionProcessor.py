from Processor import Processor
from json import load
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore


EntityWithMetadata = URIRef("https://github.com/falaimo99/syntactic_sugars/vocabulary/EntityWithMetadata")
Collection = URIRef("https://github.com/falaimo99/syntactic_sugars/vocabulary/Collection")
Manifest = URIRef("https://github.com/falaimo99/syntactic_sugars/vocabulary/Manifest")
Canvas = URIRef("https://github.com/falaimo99/syntactic_sugars/vocabulary/Canvas")


id = URIRef("https://github.com/falaimo99/syntactic_sugars/vocabulary/id")
label = URIRef("https://github.com/falaimo99/syntactic_sugars/vocabulary/label")
items_property = URIRef("https://github.com/falaimo99/syntactic_sugars/vocabulary/items")


class CollectionProcessor(Processor):
    def __init__(self):
        super().__init__()

    try:    
        def uploadData(self, path):
            
            with open(path, "r", encoding="utf-8") as f:
                
                json_data = load(f)
                
                db = Graph()

                counter_dict = {
                "Collection": 0,
                "Manifest": 0,
                "Canvas": 0,
                }
                
                def pop_graph(db, json_data):
                        
                    for key, value in json_data.items():
                        if key == "id":
                            subj = URIRef(value)

                        if key == "type" and value == "Collection":
                            obj = Collection
                            triple = (subj, RDF.type, obj)
                            db.add(triple)

                        elif key == "type" and value == "Manifest":
                            obj = Manifest
                            triple = (subj, RDF.type, obj)
                            db.add(triple)

                        elif key =="type" and value == "Canvas":
                            obj = Canvas
                            triple = (subj, RDF.type, obj)
                            db.add(triple)

                        if key == "label":
                            for key, value in value.items():
                                for element in value:    
                                    obj = Literal(element)
                                    triple = (subj, label, obj)
                                    db.add(triple)
                            
                        if key == "items":
                            for dict in value:
                                for int_key, int_value in dict.items():
                                    if int_key == "id":
                                        obj = URIRef(int_value)
                                        triple = (subj, items_property, obj)
                                        db.add(triple)
                                        pop_graph(db, dict)

                def counter(db, json_data, counter_dict):

                    for key, value in json_data.items():

                        if key == "id":
                            subj = URIRef(value)

                        if key == "type":

                            if value in counter_dict:

                                counter_dict[value] += 1
                                
                                if value == "Collection":
                                    obj = Literal(counter_dict[value])
                                    triple = (subj, id, obj)
                                    db.add(triple)

                                elif value == "Manifest":
                                    counter_dict['Canvas'] = 0
                                    obj = Literal(counter_dict[value])
                                    triple = (subj, id, obj)
                                    db.add(triple)

                                elif value == "Canvas":
                                    obj = Literal(counter_dict[value])
                                    triple = (subj, id, obj)
                                    db.add(triple)
                            
                            else:
                                counter_dict[value] = 1

                        if key == "items":

                            for dict in value:
                                counter(db, dict, counter_dict)



                def sparql_endpoint(DbPathOrUrl):
                    
                    store = SPARQLUpdateStore()

                    endpoint = self.DbPathOrUrl
                    
                    store.open((endpoint, endpoint))
                    
                    for triple in db.triples((None, None, None)):
                        store.add(triple)

                    store.close()


                counter(db, json_data, counter_dict)
                pop_graph(db, json_data)
                sparql_endpoint(self.DbPathOrUrl)

                return True
    except:
        False
