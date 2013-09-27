#!/usr/bin/env python
# Copyright (c) 2013 Carnegie Mellon University.
# All Rights Reserved.
# Redistribution and use in source and binary forms, with or without 
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice, 
# this list of conditions and the following acknowledgments and disclaimers.
# 2. Redistributions in binary form must reproduce the above copyright notice, 
# this list of conditions and the following acknowledgments and disclaimers in 
# the documentation and/or other materials provided with the distribution.
# 3. Products derived from this software may not include "Carnegie Mellon 
# University," "SEI" and/or "Software Engineering Institute" in the name of 
# such derived product, nor shall "Carnegie Mellon University," "SEI" and/or 
# "Software Engineering Institute" be used to endorse or promote products 
# derived from this software without prior written permission. For written 
# permission, please contact permission@sei.cmu.edu.
# Acknowledgments and disclaimers:
# This material is based upon work funded and supported by the Department of 
# Defense under Contract No. FA8721-05-C-0003 with Carnegie Mellon University 
# for the operation of the Software Engineering Institute, a federally funded 
# research and development center. 
#  
# Any opinions, findings and conclusions or recommendations expressed in this 
# material are those of the author(s) and do not necessarily reflect the views 
# of the United States Department of Defense. 
#  
# NO WARRANTY. THIS CARNEGIE MELLON UNIVERSITY AND SOFTWARE ENGINEERING 
# INSTITUTE MATERIAL IS FURNISHEDON AN "AS-IS" BASIS.  CARNEGIE MELLON 
# UNIVERSITY MAKES NO WARRANTIES OF ANY KIND, EITHER EXPRESSED OR IMPLIED, AS 
# TO ANY MATTER INCLUDING, BUT NOT LIMITED TO, WARRANTY OF FITNESS FOR PURPOSE 
# OR MERCHANTABILITY, EXCLUSIVITY, OR RESULTS OBTAINED FROM USE OF THE 
# MATERIAL. CARNEGIE MELLON UNIVERSITY DOES NOT MAKE ANY WARRANTY OF ANY KIND 
# WITH RESPECT TO FREEDOM FROM PATENT, TRADEMARK, OR COPYRIGHT INFRINGEMENT. 
#  
# This material has been approved for public release and unlimited distribution.
#
# Carnegie Mellon(r), CERT(r) and CERT Coordination Center(r) are registered 
# marks of Carnegie Mellon University. 
#  
# DM-0000632
def create():
    """return an empty variable dictionary"""
    return {}


def _set(outerDict, key, append):
    if outerDict is None:
        return None
    if key in outerDict:
        innerDict = outerDict[key]
    elif not append:
        return None
    else:
        innerDict = {}
        outerDict[key] = innerDict
    return innerDict


def src(varDict, source, append=True):
    """add a source to the varDict"""
    return _set(varDict, source, append)


def src_pat(varDict, source, sourcePattern, append=True):
    """add a source pattern to the varDict"""
    srcDict = src(varDict, source, append)
    return _set(srcDict, sourcePattern, append)


def src_pat_inst(varDict, source, sourcePattern,
        sourcePatternInstance, append=True):
    """add a source pattern instance to the varDict"""
    srcPatDict = src_pat(varDict, source, sourcePattern, append)
    return _set(srcPatDict, sourcePatternInstance, append)


def set_var(varDict, source, sourcePattern, sourcePatternInstance,
        sourceVariable, variable, append=True):
    """add a complete variable mapping to the varDict"""
    instDict = src_pat_inst(varDict, source, sourcePattern,
        sourcePatternInstance, append)
    if instDict is not None:
        instDict[sourceVariable] = variable


