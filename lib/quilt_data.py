#!/usr/bin/env python
#REVIEW
import os
import logging
import pprint

STATE_COMPLETED="COMPLETED"
STATE_INITIALIZED="INITIALIZED"
STATE_UNINITIALIZED="UNINITIALIZED"
STATE_ERROR="ERROR"

# Generic spec functions
def spec_set(spec, name=None):
    if spec == None:
        spec = {}
    if name != None: spec['name'] = name
    return spec
 
def spec_name_tryget(spec):
    if 'name' in spec:
        return spec['name']
    else:
        return None

def spec_name_get(spec):
    return spec['name']

def spec_name_set(spec,name):
    return spec_set(spec,name=name)

# Variable Spec functions

def var_spec_get( spec,
    name=False,
    value=False,
    sourceMapping=False,
    description=False,
    default=False
    ):
    """Accessor for information from the variable spec.  Only one parameter 
    should be set to true, otherwise first paramter that evaluates positively 
    is returned"""
    if name: return spec_name_get(name)
    if value: return spec['value']
    if sourceMapping: return spec['sourceMapping']
    if description: return spec['description']
    if default: return spec['default']
    raise Exception("Accessor not properly used")

def var_spec_tryget( spec,
    name=False,
    value=False,
    sourceMapping=False,
    description=False,
    default=False
    ):
    """Accessor for information from the variable spec.  Only one parameter 
    should be set to true, otherwise first paramter that evaluates positively 
    is returned, if no parameters evaluate positively None is returned"""
    if name: return spec_name_tryget(name) 
    if value and 'value' in spec: return spec['value'] 
    if sourceMapping and 'sourceMapping' in spec: return spec['sourceMapping'] 
    if description and 'description' in spec: return spec['description'] 
    if default and 'default' in spec: return spec['default']
    return None

def var_spec_set( spec,
    name=None,
    value=None,
    sourceMapping=None,
    description=None,
    default=None
    ):
    if spec == None:
        spec = {}
    if name != None: spec_name_set(name)
    if value != None: spec['value']= value
    if sourceMapping != None: spec['sourceMapping']= sourceMapping
    if description != None: spec['description']= description
    if default != None: spec['default']= default
    return spec

def var_spec_create(name):
    name=None,
    value=None,
    sourceMapping=sourceMapping,
    description=description,
    default=default
    ):
    return var_spec_set(
        None,
        name=name,
        value=value,
        sourceMapping=sourceMapping,
        description=description,
        default=default
        )
    
def var_specs_create():
    return {}

def var_specs_add(specs, varspec):
    if specs == None:
        specs = var_specs_create()
    specs[spec_name_get(varspec)] = varspec
    return specs

def var_specs_get(specs, varName):
    return vars[varName]


# Pattern Spec functions
def src_var_mapping_spec_create(
    name=None,
    sourceName=None,
    sourcePattern=None,
    sourceVariable=None
    ):
    return src_var_mapping_spec_set(
        None,
        name=name,
        sourceName=sourceName,
        sourcePattern=sourcePattern,
        sourceVariable=sourceVariable)

def src_var_mapping_spec_set(
    spec,
    name=None,
    sourceName=None,
    sourcePattern=None,
    sourceVariable=None
    ):
    if spec == None: spec = {}
    if name != None: spec_name_set(name)
    if sourceName != None: spec['sourceName'] = sourceName
    if sourcePattern != None: spec['sourcePattern'] = sourcePattern
    if sourceVariable != None: spec['sourceVariable'] = sourceVariable
    return spec

def src_var_mapping_spec_get(
    spec,
    name=False,
    sourceName=False,
    sourcePattern=False,
    sourceVariable=False
    ):
    """Accessor for information from the query spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned"""
    if sourceName: return spec_name_get(spec)
    if sourceName: return spec['sourceName']
    if sourcePattern: return spec['sourcePattern']
    if sourceVariable: return spec['sourceVariable']
    raise Exception("Accessor not properly used")

def src_var_mapping_specs_create():
    return []

def src_var_mapping_specs_add(mappingSpecs=None, mappingSpec):
    if mappingSpecs = None:
        mappingSpecs = src_var_mapping_specs_create()
    mappingSpecs.append(mappingSpec)
    return mappingSpecs
    

def pat_spec_create(
    name=None,
    mappings=None,
    variables=None
    ):
    return src_pat_spec_set(
        None,
        name=name,
        mappings=mappings,
        variables=variables)

def pat_spec_set(
    spec,
    name=None,
    mappings=None,
    variables=None
    ):
    if spec == None: spec = {}
    if name != None: spec_name_set(name)
    if mappings != None: spec['mappings'] = mappings
    if variables != None: spec['variables']= variables
    return spec


def pat_spec_var_get( spec, varName):
    return var_specs_get(pat_spec_vars_get(spec), varName)

    
    
def pat_specs_create():
    return {}

def pat_specs_add(patSpecs, patSpec):
    if patSpecs == None:
        patSpecs = pat_specs_create()
    patSpecs[pat_spec_name_get(patSpec)] = patSpec
    return patSpecs
    
def pat_specs_get(patSpecs, patName):
    return patSpecs[patName]    

    
# Query Spec functions
# Query's are basically specialized patterns, but I am not trying to reinvent
# inheritance here.  So all fields are just duplicated rather than trying to 
# do some complicated chaining to pattern_spec funcitons.

def query_spec_create(
    name=None,
    state=None,
    patternName=None,
    notificationEmail=None,
    results=None,
    variables=None
    ):
    return query_spec_set(
        None,
        name=name,
        state=state,
        patternName=patternName,
        notificationEmail=notificationEmail,
        results=results,
        variables=variables)

def query_spec_set( 
    spec,
    name=None,
    state=None,
    patternName=None,
    notificationEmail=None,
    results=None,
    variables=None
    ):
    if spec == None: spec = {}
    if name != None: spec_name_set(name)
    if state != None: spec['state']= state
    if patternName != None: spec['patternName']= patternName
    if results != None: spec['results']= results
    if variables != None: spec['variables']= variables
    return spec
    
def query_spec_get( 
    spec,
    name=False,
    state=False,
    patternName=False,
    notificationEmail=False,
    results=False,
    variables=False
    ):
    """Accessor for information from the query spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned"""
    if name: return spec_name_get(name)
    if state: return spec['state']
    if patternName: return spec['patternName']
    if notificationEmail: return spec['notificationEmail']
    if results: return spec['results']
    if variables: return spec['variables']
    raise Exception("Accessor not properly used")
    
def query_spec_tryget( 
    spec,
    name=False,
    state=False,
    patternName=False,
    notificationEmail=False,
    results=False,
    variables=False
    ):
    """Accessor for information from the query spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned.  If value is not present in spec None is returned"""
    if name: return spec_name_tryget(name)
    if state and 'state' in spec: return spec['state']
    if patternName and 'patternName' in spec: return spec['patternName']
    if notificationEmail and 'notificationEmail' in spec: return spec['notificationEmail']
    if results and 'results' in spec: return spec['results']
    if variables and 'variables' in spec: return spec['variables']
    return None
    

query_spec_var_get = pat_spec_var_get

def query_specs_create():
    return {}

def query_specs_add(querySpecs, querySpec)
    if querySpecs == None:
        querySpecs = query_specs_create()
    querySpecs[query_spec_get(querySpec,name=True)] = querySpec
    return querySpecs

def query_specs_get(querySpecs, querySpecName):
    return querySpecs[querySpecName]

def query_specs_del(querySpecs, querySpecName):
    spec = query_specs_get(querySpecs, querySpecName)
    del querySpecs[querySpecName]
    return spec

# Source Pattern Spec functions
def src_pat_spec_create(
    name=None,
    variables=None
    ):
    return src_pat_spec_set(
        None,
        name=name,
        variables=variables)

def src_pat_spec_set(
    spec,
    name=None,
    variables=None
    ):
    if spec == None: spec = {}
    if name != None: spec_name_set(name)
    if variables != None: spec['variables']= variables
    return spec

def src_pat_spec_get(
    spec,
    name=False,
    variables=False
    ):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned"""
    if name: return spec_name_get(name)
    if variables: return spec['variables']
    raise Exception("Accessor not properly used")

def src_pat_spec_tryget(
    spec,
    name=False,
    variables=False
    ):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned.  If value is not present in spec, None is returned"""
    if name: return spec_name_tryget(name)
    if variables and 'variables' in spec: return spec['variables']
    return None

def src_pat_specs_create():
    return {}

def src_pat_specs_get(srcPatSpecs, patName):
    return srcPatSpecs[patName]

# source query section

def src_query_spec_create(
    name=None,
    srcPatternName=None,
    variables=None
    ):
    return src_pat_spec_set(
        None,
        srcPatternName=srcPatternName
        name=name,
        variables=variables)

def src_query_spec_set(
    spec,
    name=None,
    srcPatternName=None,
    variables=None
    ):
    if spec == None: spec = {}
    if name != None: spec_name_set(name)
    if srcPatternName != None: spec['srcPatternName']= srcPatternName
    if variables != None: spec['variables']= variables
    return spec

def src_query_spec_get(
    spec,
    name=False,
    srcPatternName=False,
    variables=False
    ):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned"""
    if name: return spec_name_get(name)
    if variables: return spec['variables']
    if srcPatternName: return spec['srcPatternName']
    raise Exception("Accessor not properly used")

def src_query_spec_tryget(
    spec,
    name=False,
    srcPatternName=False,
    variables=False
    ):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned.  If value is not present in spec, None is returned"""
    if name: return spec_name_tryget(name)
    if variables and 'variables' in spec: return spec['variables']
    if srcPatternName and 'srcPatternName' in spec: return spec['srcPatternName']
    return None


def src_query_specs_create():
    return {}

def src_query_specs_add(srcQuerySpecs=None, srcQuerySpec):
    if srcQuerySpecs == None:
        srcQuerySpecs = src_query_specs_create()
    name = src_query_spec_get(srcQuerySpec,name=True)
    srcQuerySpecs[name] = srcQuerySpec
    return srcQuerySpec

def src_spec_set(spec,
    name=None,
    sourcePatterns=None
    ):
    if spec == None:
        spec = src_spec_create()
    if name != None: spec_name_set(spec,name)
    if sourcePatterns != None: spec['sourcePatterns'] = sourcePatterns
    return spec

def src_spec_get(spec,
    name=False,
    sourcePatterns=False
    ):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned"""
    if name: return spec_name_get(spec)
    if sourcePatterns: return spec['sourcePatterns']
    raise Exception("Accessor not properly used")

    
def src_spec_tryget(spec,
    name=False,
    sourcePatterns=False
    ):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first paramter that evaluates positively is 
    returned.  If no arguments evaluate positively, None will be returned"""
    if name: return spec_name_tryget(spec)
    if sourcePatterns and 'sourcePatterns' in spec: return spec['sourcePatterns']
    return None
    
def src_spec_create(cfgStr=None,cfgSection=None):
    if cfgStr:
        # we allow the user certain shortcuts when defining a spec,
        # we now go through the spec they defined and translate the
        # user input to the official sourceSpec schema
        #TODO harden against eval
        cfgSrcSpec = eval(cfgStr)

        if src_spec_tryget(name=True) == None:
            # lazy user did not provide name, use the config section as name
            src_spec_set(name=cfgSection)
        
        cfgSrcPatSpec = src_spec_get(cfgSrcSpec,sourcePatterns=True)
        cfgSrcPatVarSpecs = src_pat_spec_get(cfgSrcPatSpec,variables=True)
        for name,val in cfgSrcPatVarSpecs:
            # lazy user only specified description, convert to a variable spec
            if type(val)==str:
!!!!!!!!!!!!!!!!!!!!!!!
                vspec = var_specs_create( name=name, description=val)
                var_specs_add(cfgSrcPatVarSpecs, vspec)
        return cfgSrcSpec
    return {}

def src_specs_create():
    return {}

def src_specs_add(srcSpecs=None,srcSpec):
    if srcSpecs == None:
        srcSpecs = src_specs_create()
    
    
