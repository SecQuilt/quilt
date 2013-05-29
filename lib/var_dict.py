#!/usr/bin/env python

def create():
    """return an empty variable dictionary"""
    return {}

def _set(outerDict, key,append):
    if outerDict == None:
        return None
    if key in outerDict:
        innerDict = outerDict[key]
    elif append == False:
        return None
    else:
        innerDict = {}
        outerDict[key] = innerDict
    return innerDict

def src(varDict, source, append=True):
    """add a source to the varDict"""
    return _set(varDict, source,append)


def src_pat(varDict, source, sourcePattern, append=True ):
    """add a source pattern to the varDict"""
    srcDict = src(varDict, source,append)
    return _set(srcDict, sourcePattern,append)

def src_pat_inst(varDict, source, sourcePattern,
        sourcePatternInstance,append=True):
    """add a source pattern instance to the varDict"""
    srcPatDict = src_pat(varDict, source, sourcePattern,append)
    return _set(srcPatDict, sourcePatternInstance,append)

def set_var(varDict, source, sourcePattern, sourcePatternInstance,
        sourceVariable, variable, append=True):
    """add a complete variable mapping to the varDict"""
    instDict = src_pat_inst(varDict, source, sourcePattern, 
            sourcePatternInstance,append)
    if instDict != None:
        instDict[sourceVariable] = variable


