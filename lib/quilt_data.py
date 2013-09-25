#!/usr/bin/env python
# Copyright 2013 Carnegie Mellon University 
#  
# This material is based upon work funded and supported by the Department of Defense under Contract No. FA8721-
# 05-C-0003 with Carnegie Mellon University for the operation of the Software Engineering Institute, a federally 
# funded research and development center. 
#  
# Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and 
# do not necessarily reflect the views of the United States Department of Defense. 
#  
# NO WARRANTY. THIS CARNEGIE MELLON UNIVERSITY AND SOFTWARE ENGINEERING INSTITUTE 
# MATERIAL IS FURNISHEDON AN "AS-IS" BASIS. CARNEGIE MELLON UNIVERSITY MAKES NO 
# WARRANTIES OF ANY KIND, EITHER EXPRESSED OR IMPLIED, AS TO ANY MATTER INCLUDING, 
# BUT NOT LIMITED TO, WARRANTY OF FITNESS FOR PURPOSE OR MERCHANTABILITY, 
# EXCLUSIVITY, OR RESULTS OBTAINED FROM USE OF THE MATERIAL. CARNEGIE MELLON 
# UNIVERSITY DOES NOT MAKE ANY WARRANTY OF ANY KIND WITH RESPECT TO FREEDOM FROM 
# PATENT, TRADEMARK, OR COPYRIGHT INFRINGEMENT. 
#  
# This material has been approved for public release and unlimited distribution except as restricted below. 
#  
# Internal use:* Permission to reproduce this material and to prepare derivative works from this material for internal 
# use is granted, provided the copyright and "No Warranty" statements are included with all reproductions and 
# derivative works. 
#  
# External use:* This material may be reproduced in its entirety, without modification, and freely distributed in 
# written or electronic form without requesting formal permission. Permission is required for any other external and/or 
# commercial use. Requests for permission should be directed to the Software Engineering Institute at 
# permission@sei.cmu.edu. 
#  
# * These restrictions do not apply to U.S. government entities. 
#  
# Carnegie Mellon(r), CERT(r) and CERT Coordination Center(r) are registered marks of Carnegie Mellon University. 
#  
# DM-0000632 
import logging
from string import Template

STATE_UNINITIALIZED = "UNINITIALIZED"
STATE_INITIALIZED = "INITIALIZED"
STATE_COMPLETED = "COMPLETED"
STATE_ACTIVE = "ACTIVE"
STATE_ERROR = "ERROR"

# Generic spec functions
def spec_set(spec, name=None):
    if spec is None:
        spec = {}
    if name is not None: spec['name'] = name
    return spec


def spec_name_tryget(spec):
    if 'name' in spec:
        return spec['name']
    else:
        return None


def spec_name_get(spec):
    return spec['name']


def spec_name_set(spec, name):
    return spec_set(spec, name=name)

# Variable Spec functions

def var_spec_get(spec,
                 name=False,
                 value=False,
                 #sourceMapping=False,
                 description=False,
                 default=False
):
    """Accessor for information from the variable spec.  Only one parameter 
    should be set to true, otherwise first parameter that evaluates positively
    is returned"""
    if name: return spec_name_get(spec)
    if value: return spec['value']
    #if sourceMapping: return spec['sourceMapping']
    if description: return spec['description']
    if default: return spec['default']
    raise Exception("Accessor not properly used")


def var_spec_tryget(spec,
                    name=False,
                    value=False,
                    #sourceMapping=False,
                    description=False,
                    default=False
):
    """Accessor for information from the variable spec.  Only one parameter 
    should be set to true, otherwise first parameter that evaluates positively
    is returned, if no parameters evaluate positively None is returned"""
    if name: return spec_name_tryget(spec)
    if value and 'value' in spec: return spec['value']
    #if sourceMapping and 'sourceMapping' in spec: return spec['sourceMapping'] 
    if description and 'description' in spec: return spec['description']
    if default and 'default' in spec: return spec['default']
    return None


def var_spec_set(spec,
                 name=None,
                 value=None,
                 #sourceMapping=None,
                 description=None,
                 default=None
):
    if spec is None:
        spec = {}
    if name is not None: spec_name_set(spec, name)
    if value is not None: spec['value'] = value
    #if sourceMapping is not None: spec['sourceMapping']= sourceMapping
    if description is not None: spec['description'] = description
    if default is not None: spec['default'] = default
    return spec


def var_spec_create(
        name=None,
        value=None,
        #sourceMapping=None,
        description=None,
        default=None
):
    return var_spec_set(
        None,
        name=name,
        value=value,
        #sourceMapping=sourceMapping,
        description=description,
        default=default
    )


def var_specs_create():
    return {}


def var_specs_add(specs, varspec):
    if specs is None:
        specs = var_specs_create()
    specs[var_spec_get(varspec, name=True)] = varspec
    return specs


def var_specs_get(specs, varName):
    return specs[varName]


def var_specs_tryget(specs, varName):
    if specs is None or varName not in specs:
        return None
    return specs[varName]

# Pattern Spec functions
def src_var_mapping_spec_create(
        name=None,
        sourceName=None,
        sourcePattern=None,
        sourcePatternInstance=None,
        sourceVariable=None
):
    return src_var_mapping_spec_set(
        None,
        name=name,
        sourceName=sourceName,
        sourcePattern=sourcePattern,
        sourcePatternInstance=sourcePatternInstance,
        sourceVariable=sourceVariable)


def src_var_mapping_spec_set(
        spec,
        name=None,
        sourceName=None,
        sourcePattern=None,
        sourcePatternInstance=None,
        sourceVariable=None
):
    if spec is None: spec = {}
    if name is not None: spec_name_set(spec, name)
    if sourceName is not None: spec['sourceName'] = sourceName
    if sourcePattern is not None: spec['sourcePattern'] = sourcePattern
    if sourcePatternInstance is not None: spec[
        'sourcePatternInstance'] = sourcePatternInstance
    if sourceVariable is not None: spec['sourceVariable'] = sourceVariable
    return spec


def src_var_mapping_spec_get(
        spec,
        name=False,
        sourceName=False,
        sourcePattern=False,
        sourcePatternInstance=False,
        sourceVariable=False
):
    """Accessor for information from the query spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned"""
    if name: return spec_name_get(spec)
    if sourceName: return spec['sourceName']
    if sourcePattern: return spec['sourcePattern']
    if sourcePatternInstance: return spec['sourcePatternInstance']
    if sourceVariable: return spec['sourceVariable']
    raise Exception("Accessor not properly used")


def src_var_mapping_spec_tryget(
        spec,
        name=False,
        sourceName=False,
        sourcePattern=False,
        sourcePatternInstance=False,
        sourceVariable=False
):
    """Accessor for information from the query spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned.  If no parameters evaluate positively, or object has not been
    sey, None is returned"""
    if name: return spec_name_tryget(spec)
    if sourceName and 'sourceName' in spec: return spec['sourceName']
    if sourcePattern and 'sourcePattern' in spec: return spec['sourcePattern']
    if sourcePatternInstance and 'sourcePatternInstance' in spec: return spec[
        'sourcePatternInstance']
    if sourceVariable and 'sourceVariable' in spec: return spec[
        'sourceVariable']
    return None


def src_var_mapping_specs_create():
    return []


def src_var_mapping_specs_add(mappingSpecs, mappingSpec):
    if mappingSpecs is None:
        mappingSpecs = src_var_mapping_specs_create()
    mappingSpecs.append(mappingSpec)
    return mappingSpecs


def pat_spec_set(
        spec,
        name=None,
        mappings=None,
        code=None,
        variables=None
):
    if spec is None: spec = {}
    if name is not None: spec_name_set(spec, name)
    if mappings is not None: spec['mappings'] = mappings
    if code is not None: spec['code'] = code
    if variables is not None: spec['variables'] = variables
    return spec


def pat_spec_create(
        name=None,
        mappings=None,
        code=None,
        variables=None
):
    return pat_spec_set(
        None,
        name=name,
        mappings=mappings,
        code=code,
        variables=variables)


def pat_spec_get(
        spec,
        name=False,
        mappings=False,
        code=False,
        variables=False
):
    """Accessor for information from the pattern spec.  
    Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned"""
    if name: return spec_name_get(spec)
    if mappings: return spec['mappings']
    if code: return spec['code']
    if variables: return spec['variables']
    raise Exception("Accessor not properly used")


def pat_spec_tryget(
        spec,
        name=False,
        mappings=False,
        code=False,
        variables=False
):
    """Accessor for information from the pattern spec.  
    Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned.  If no parameter evaluates to True, None is returned"""
    if name: return spec_name_tryget(spec)
    if mappings and 'mappings' in spec: return spec['mappings']
    if code and 'code' in spec: return spec['code']
    if variables and 'variables' in spec: return spec['variables']
    return None

# shortcut function dereferences variables for you
def pat_spec_var_get(spec, varName):
    return var_specs_get(pat_spec_get(spec, variables=True), varName)


def pat_specs_create():
    return {}


def pat_specs_add(patSpecs, patSpec):
    if patSpecs is None:
        patSpecs = pat_specs_create()
    name = pat_spec_get(patSpec, name=True)
    patSpecs[name] = patSpec
    return patSpecs


def pat_specs_get(patSpecs, patName):
    return patSpecs[patName]


# Query Spec functions
# Query's are basically specialized patterns, but I am not trying to reinvent
# inheritance here.  So all fields are just duplicated rather than trying to 
# do some complicated chaining to pattern_spec functions.

def query_spec_set(
        spec,
        name=None,
        state=None,
        patternName=None,
        notificationEmail=None,
        results=None,
        sourceQuerySpecs=None,
        variables=None
):
    if spec is None: spec = {}
    if name is not None: spec_name_set(spec, name)
    if state is not None: spec['state'] = state
    if patternName is not None: spec['patternName'] = patternName
    if notificationEmail is not None: spec[
        'notificationEmail'] = notificationEmail
    if results is not None: spec['results'] = results
    if sourceQuerySpecs is not None: spec['sourceQuerySpecs'] = sourceQuerySpecs
    if variables is not None: spec['variables'] = variables
    return spec


def query_spec_create(
        name=None,
        state=None,
        patternName=None,
        notificationEmail=None,
        results=None,
        sourceQuerySpecs=None,
        variables=None
):
    return query_spec_set(
        None,
        name=name,
        state=state,
        patternName=patternName,
        notificationEmail=notificationEmail,
        results=results,
        sourceQuerySpecs=sourceQuerySpecs,
        variables=variables)


def query_spec_get(
        spec,
        name=False,
        state=False,
        patternName=False,
        notificationEmail=False,
        results=False,
        sourceQuerySpecs=False,
        variables=False
):
    """Accessor for information from the query spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned"""
    if name: return spec_name_get(spec)
    if state: return spec['state']
    if patternName: return spec['patternName']
    if notificationEmail: return spec['notificationEmail']
    if results: return spec['results']
    if sourceQuerySpecs: return spec['sourceQuerySpecs']
    if variables: return spec['variables']
    raise Exception("Accessor not properly used")


def query_spec_tryget(
        spec,
        name=False,
        state=False,
        patternName=False,
        notificationEmail=False,
        results=False,
        sourceQuerySpecs=False,
        variables=False
):
    """Accessor for information from the query spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned.  If value is not present in spec None is returned"""
    if name: return spec_name_tryget(spec)
    if state and 'state' in spec: return spec['state']
    if patternName and 'patternName' in spec: return spec['patternName']
    if notificationEmail and 'notificationEmail' in spec: return spec[
        'notificationEmail']
    if results and 'results' in spec: return spec['results']
    if sourceQuerySpecs and 'sourceQuerySpecs' in spec: return spec[
        'sourceQuerySpecs']
    if variables and 'variables' in spec: return spec['variables']
    return None


query_spec_var_get = pat_spec_var_get


def query_specs_create():
    return {}


def query_specs_add(querySpecs, querySpec):
    if querySpecs is None:
        querySpecs = query_specs_create()
    querySpecs[query_spec_get(querySpec, name=True)] = querySpec
    return querySpecs


def query_specs_get(querySpecs, querySpecName):
    return querySpecs[querySpecName]


def query_specs_tryget(querySpecs, querySpecName):
    if querySpecName in querySpecs:
        return querySpecs[querySpecName]
    return None


def query_specs_del(querySpecs, querySpecName):
    spec = query_specs_get(querySpecs, querySpecName)
    del querySpecs[querySpecName]
    return spec


def query_specs_trydel(querySpecs, querySpecName):
    spec = query_specs_tryget(querySpecs, querySpecName)
    if spec is not None:
        del querySpecs[querySpecName]
    return spec

# Source Pattern Spec functions

def src_pat_spec_set(
        spec,
        name=None,
        ordered=None,
        variables=None
):
    if spec is None: spec = {}
    if name is not None: spec_name_set(spec, name)
    if ordered is not None: spec['ordered'] = ordered
    if variables is not None: spec['variables'] = variables
    return spec


def src_pat_spec_create(
        name=None,
        ordered=None,
        variables=None
):
    return src_pat_spec_set(
        None,
        name=name,
        ordered=ordered,
        variables=variables)


def src_pat_spec_get(
        spec,
        name=False,
        ordered=False,
        variables=False
):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned"""
    if name: return spec_name_get(spec)
    if ordered: return spec['ordered']
    if variables: return spec['variables']
    raise Exception("Accessor not properly used")


def src_pat_spec_tryget(
        spec,
        name=False,
        ordered=False,
        variables=False
):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned.  If value is not present in spec, None is returned"""
    if name: return spec_name_tryget(spec)
    if ordered and 'ordered' in spec: return spec['ordered']
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
        srcPatternInstance=None,
        state=None,
        source=None,
        ordered=None,
        variables=None
):
    return src_query_spec_set(
        None,
        name=name,
        srcPatternName=srcPatternName,
        srcPatternInstance=srcPatternInstance,
        state=state,
        source=source,
        ordered=ordered,
        variables=variables)


def src_query_spec_set(
        spec,
        name=None,
        srcPatternName=None,
        srcPatternInstance=None,
        state=None,
        source=None,
        ordered=None,
        variables=None
):
    if spec is None: spec = {}
    if name is not None: spec_name_set(spec, name)
    if srcPatternName is not None: spec['srcPatternName'] = srcPatternName
    if srcPatternInstance is not None: spec[
        'srcPatternInstance'] = srcPatternInstance
    if state is not None: spec['state'] = state
    if source is not None: spec['source'] = source
    if ordered is not None: spec['ordered'] = ordered
    if variables is not None: spec['variables'] = variables
    return spec


def src_query_spec_get(
        spec,
        name=False,
        srcPatternName=False,
        srcPatternInstance=False,
        state=False,
        source=False,
        ordered=False,
        variables=False
):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned"""
    if name: return spec_name_get(spec)
    if srcPatternName: return spec['srcPatternName']
    if srcPatternInstance: return spec['srcPatternInstance']
    if state: return spec['state']
    if source: return spec['source']
    if ordered: return spec['ordered']
    if variables: return spec['variables']
    raise Exception("Accessor not properly used")


def src_query_spec_tryget(
        spec,
        name=False,
        srcPatternName=False,
        srcPatternInstance=False,
        state=False,
        source=False,
        ordered=False,
        variables=False
):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned.  If value is not present in spec, None is returned"""
    if name: return spec_name_tryget(spec)
    if srcPatternName and 'srcPatternName' in spec: return spec[
        'srcPatternName']
    if srcPatternInstance and 'srcPatternInstance' in spec: return spec[
        'srcPatternInstance']
    if state and 'state' in spec: return spec['state']
    if source and 'source' in spec: return spec['source']
    if ordered and 'ordered' in spec: return spec['ordered']
    if variables and 'variables' in spec: return spec['variables']
    return None


def src_query_specs_create():
    return {}


def src_query_specs_add(srcQuerySpecs, srcQuerySpec):
    if srcQuerySpecs is None:
        srcQuerySpecs = src_query_specs_create()
    name = src_query_spec_get(srcQuerySpec, name=True)
    srcQuerySpecs[name] = srcQuerySpec
    return srcQuerySpecs


def src_query_specs_get(srcQuerySpecs, srcQueryName):
    return srcQuerySpecs[srcQueryName]


def src_spec_set(spec,
                 name=None,
                 sourcePatterns=None
):
    if spec is None:
        spec = src_spec_create()
    if name is not None: spec_name_set(spec, name)
    if sourcePatterns is not None: spec['sourcePatterns'] = sourcePatterns
    return spec


def src_spec_get(spec,
                 name=False,
                 sourcePatterns=False
):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned"""
    if name: return spec_name_get(spec)
    if sourcePatterns: return spec['sourcePatterns']
    raise Exception("Accessor not properly used")


def src_spec_tryget(spec,
                    name=False,
                    sourcePatterns=False
):
    """Accessor for information from the spec.  Only one parameter should
    be set to true, otherwise first parameter that evaluates positively is
    returned.  If no arguments evaluate positively, None will be returned"""
    if name: return spec_name_tryget(spec)
    if sourcePatterns and 'sourcePatterns' in spec: return spec[
        'sourcePatterns']
    return None


def src_spec_create(cfgStr=None, cfgSection=None):
    if cfgStr is not None:
        # we allow the user certain shortcuts when defining a spec,
        # we now go through the spec they defined and translate the
        # user input to the official sourceSpec schema
        #TODO harden against eval
        cfgSrcSpec = eval(cfgStr)

        if src_spec_tryget(cfgSrcSpec, name=True) is None:
            # lazy user did not provide name, use the config section as name
            src_spec_set(cfgSrcSpec, name=cfgSection)

        cfgSrcPatSpecs = src_spec_get(cfgSrcSpec, sourcePatterns=True)
        for srcPatName, srcPatSpec in cfgSrcPatSpecs.items():

            # We need to make sure that the name is set as a member of
            # the srcPatSpec.  If user did not specify it just set it
            # to the key value
            srcPatSpecName = src_pat_spec_tryget(srcPatSpec, name=True)
            if srcPatSpecName is not None and srcPatSpecName != srcPatName:
                # user has specified inconsistent names for a source pattern
                # throw an error
                raise Exception("Inconstant names specified for source" +
                                " pattern ( " + srcPatName + ", " + srcPatSpecName + " )")
            src_pat_spec_set(srcPatSpec, name=srcPatName)


            # check if the user did not set the 'ordered' field, we will
            # default it to true
            ordered = src_pat_spec_tryget(srcPatSpec, ordered=True)
            if ordered is None:
                ordered = True
                # set whether or not this spec guarantees return of ordered
            # results
            src_pat_spec_set(srcPatSpec, ordered=ordered)

            cfgSrcPatVarSpecs = src_pat_spec_get(srcPatSpec, variables=True)
            for name, val in cfgSrcPatVarSpecs.items():
                # lazy user only specified description, convert to a variable spec
                if type(val) == str:
                    vspec = var_spec_create(name=name, description=val)

                    var_specs_add(cfgSrcPatVarSpecs, vspec)
                elif type(val) == dict:
                    # check if lazy user did not set name for variable
                    # logging.debug ("Val is : " + str(val))
                    srcVarName = var_spec_tryget(val, name=True)

                    # user may have specified conflicting name for key and 
                    # actual name, guard against this stupidity
                    if srcVarName is not None and srcVarName != name:
                        raise Exception("Inconstant names specified for " +
                                        "source pattern variable: ( " + str(
                            srcVarName) +
                                        ", " + str(name) + " )")
                        # set the official name into the variable spec
                    var_spec_set(val, name=name)
                else:
                    raise Exception("Do not know how to specify source " +
                                    "variable with object of type: " + str(
                        type(val)))

                logging.info("Variable created: " + str(name))

        return cfgSrcSpec
    return {}


def src_specs_create():
    return {}


def src_specs_add(srcSpecs, srcSpec):
    if srcSpecs is None:
        srcSpecs = src_specs_create()
    srcSpecs[src_spec_get(srcSpec, name=True)] = srcSpec
    return srcSpecs


def generate_var_value_dict(patternSpec, querySpec):
    """
    return a dictionary mapping variable names to replacement values using
    the variable specs in the patternSpec and/or the querySpec.  Either
    argument may be None.  Variable values are determined in the following
    order: pattern variable default, query variable default, query variable
    value.
    """

    replacements = {}

    # if a pattern was specified
    if patternSpec is not None:

        #if this pattern has some variables defined
        patVars = pat_spec_tryget(patternSpec, variables=True)
        if patVars is not None:

            # iterate the variables in the list
            for varName, patVarSpec in patVars.items():
                value = var_spec_tryget(patVarSpec, default=True)
                # if a default value is stored in the pattern variable
                if value is not None:
                    # record this variable name value mapping
                    replacements[varName] = value

    # if a query was specified
    if querySpec is not None:
        # iterate the query variables if the exist
        querySpecVars = query_spec_tryget(querySpec, variables=True)
        if querySpecVars is not None:
            for varName, queryVarSpec in querySpecVars.items():
                # record value if a default exists
                value = var_spec_tryget(queryVarSpec, default=True)
                if value is not None:
                    replacements[varName] = value
                    # record value if a value exists
                value = var_spec_tryget(queryVarSpec, value=True)
                if value is not None:
                    replacements[varName] = value

    return replacements


def generate_query_code(patternSpec, querySpec):
    """
    Search and replace variables that have a known value from the pattern code
    """
    code = pat_spec_get(patternSpec, code=True)
    # create a variable replacement map from query spec
    replacements = generate_var_value_dict(patternSpec, querySpec)
    # create Template based on pattern code
    codeTemplate = Template(code)
    # substitute variables in pattern code's template
    code = codeTemplate.substitute(replacements)

    return code


