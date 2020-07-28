#!/usr/bin/env python3

import sys
from rdflib import Graph, Namespace, URIRef, Literal
import rdflib

###############################################################################
####################################################################### helpers

def insert (triple):
    if triple in g: return
    g.add(triple)

def restrict (subs, pred, objs):
    for sub in subs:
        insert( (pred, RDFS.domain, sub) )
    for obj in objs:
        insert( (pred, RDFS.range, obj) )

def create_property (ns, name, property_type_names, subs=None, objs=None, parent=None):
    if not type(property_type_names)==list: property_type_names = [property_type_names]
    if subs==None: subs = []
    if objs==None: objs = []
    property_types = {
      'rdf':    RDF.Property,
      'object': OWL.ObjectProperty,
      'data':   OWL.DatatypeProperty,
    }
    
    entity = ns[name]
    for property_type_name in property_type_names:
        insert( (entity, RDF.type, property_types[property_type_name]) )
    if parent:
        insert( (entity, RDFS.subPropertyOf, parent) )
    restrict(subs, entity, objs)
    
    return entity

def set_cardinality (ns, type_entity, property_entity, cardmin=None, cardmax=None):
    if cardmin:
        cardmin_entity = ns['_%s_cardmin' % type_entity.split('#')[-1]]
        insert( (type_entity, OWL.equivalentClass, cardmin_entity) )
        insert( (cardmin_entity, RDF.type, OWL.Restriction) )
        insert( (cardmin_entity, OWL.onProperty, property_entity) )
        insert( (cardmin_entity, OWL.minCardinality, Literal(cardmin)) )
    if cardmax:
        cardmax_entity = ns['_%s_cardmax' % type_entity.split('#')[-1]]
        insert( (type_entity, OWL.equivalentClass, cardmax_entity) )
        insert( (cardmax_entity, RDF.type, OWL.Restriction) )
        insert( (cardmax_entity, OWL.onProperty, property_entity) )
        insert( (cardmax_entity, OWL.maxCardinality, Literal(cardmax)) )

def create_class (ns, name, parent=None, label=None):
    if parent==None: parent = OWL.Class
    
    entity = ns[name]
    insert( (entity, RDFS.subClassOf, parent) )
    
    if label:
        insert( (entity, RDF.label, Literal(label)) )
    
    return entity

###############################################################################
#################################################################### main start

# guard: commandline arguments
if len(sys.argv) != 2:
    print('Syntax: %s OUTPUT_FILE' % sys.argv[0])
    print('        %s grundfos-bidi.ttl' % sys.argv[0])
    sys.exit(1)

output_filename = sys.argv[1]

# external namespaces
RDF   = Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
RDFS  = Namespace('http://www.w3.org/2000/01/rdf-schema#')
OWL   = Namespace('http://www.w3.org/2002/07/owl#')
XSD   = Namespace('http://www.w3.org/2001/XMLSchema#')
BRICK = Namespace('https://brickschema.org/schema/1.1.0/Brick#')

GFB   = Namespace('http://ss.sdu.dk/test/grundstrup-bidi-ontology/20200727/#')

g = Graph()

g.bind('rdf'   , RDF)
g.bind('rdfs'  , RDFS)
g.bind('owl'   , OWL)
g.bind('xsd'   , XSD)
g.bind('brick' , BRICK)
g.bind('gfb', GFB)

###############################################################################
###################################################################### ontology

# classes
Heat_Exchanger               = create_class(GFB, 'CoffeeMachine'               , BRICK['Heat_Exchanger']              , 'Heat exchanger')
Hot_Water_Tank               = create_class(GFB, 'Hot_Water_Tank'              , GFB['Heat_Exchanger']                , 'Domestic hot water tank')
Heat_Sensor                  = create_class(GFB, 'Heat_Sensor'                 , BRICK['Sensor']                      , 'Heat meter virtual sensor')
Water_Flow_Sensor            = create_class(GFB, 'Water_Flow_Sensor'           , BRICK['Flow_Sensor']                 , 'Water flow sensor')
Water_Temperature_Sensor     = create_class(GFB, 'Water_Temperature_Sensor'    , BRICK['Temperature_Sensor']          , 'Water temperature sensor')
Pump                         = create_class(GFB, 'Pump'                        , BRICK['Pump']                        , 'Water pump')
Valve                        = create_class(GFB, 'Valve'                       , BRICK['Valve']                       , 'Electronically controlled valve')
Bypass_Valve                 = create_class(GFB, 'Bypass_Valve'                , GFB['Valve']                         , 'Electronically controlled bypass valve')
Differential_Pressure_Sensor = create_class(GFB, 'Differential_Pressure_Sensor', BRICK['Differential_Pressure_Sensor'], 'Differential pressure sensor')

# properties
controls = create_property(GFB, 'controls', 'object', [Water_Temperature_Sensor], [Heat_Sensor], parent=BRICK['controls'])
feedsWater = create_property(GFB, 'feedsWater', 'object', [BRICK['Equipment'], BRICK['Point']], [BRICK['Equipment'], BRICK['Point']], parent=BRICK['feeds'])
feedsHeatedWater       = create_property(GFB, 'feedsHeatedWater'      , 'object', [], [], parent=feedsWater)
feedsSupplyHeatedWater = create_property(GFB, 'feedsSupplyHeatedWater', 'object', [], [], parent=feedsHeatedWater)
feedsReturnHeatedWater = create_property(GFB, 'feedsReturnHeatedWater', 'object', [], [], parent=feedsHeatedWater)
feedsDistrictHeatedWater       = create_property(GFB, 'feedsDistrictHeatedWater'      , 'object', [], [], parent=feedsHeatedWater)
feedsSupplyDistrictHeatedWater = create_property(GFB, 'feedsSupplyDistrictHeatedWater', 'object', [], [], parent=feedsDistrictHeatedWater)
feedsReturnDistrictHeatedWater = create_property(GFB, 'feedsReturnDistrictHeatedWater', 'object', [], [], parent=feedsDistrictHeatedWater)
feedsDomesticHeatedWater       = create_property(GFB, 'feedsDomesticHeatedWater'      , 'object', [], [], parent=feedsHeatedWater)
feedsSupplyDomesticHeatedWater = create_property(GFB, 'feedsSupplyDomesticHeatedWater', 'object', [], [], parent=feedsDomesticHeatedWater)
feedsReturnDomesticHeatedWater = create_property(GFB, 'feedsReturnDomesticHeatedWater', 'object', [], [], parent=feedsDomesticHeatedWater)

###############################################################################
######################################################################### store

g.serialize(output_filename, 'turtle')

########################################################################### EOF
###############################################################################

