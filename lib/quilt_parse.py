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
#REVIEW
import logging
import ast
import var_dict

#   def tree_print(node, depth):

#       basename = type(node).__name__

#       s = ""
#       for i in range(depth):
#           i = i
#           s += " | "

#       s += basename

#       for field, value in ast.iter_fields(node):
#           if (field == 'id'):
#               s += ": " + value + "("
#           elif (field == 's'):
#               s += ": " + value + ","

#       logging.debug(s)
#       for n in ast.iter_child_nodes(node):
#           tree_print(n,depth+1)

class SourceVisitor(ast.NodeVisitor):
    """
    Provide virtual callbacks for each appropriate node of the 
    AST.  Responsible for collecting sources referenced in pattern
    code
    """

    def __init__(self):
        # chain call to parent constructor
        ast.NodeVisitor.__init__(self)
        # initialize sourcePattern dictionary member
        self.varDict = var_dict.create()

    def visit_Call(self, node):
    # chain call to parent class generic visitor to assure
        #   recursive operations
        super(SourceVisitor, self).generic_visit(node)

        # AST for a call to a source, looks like this.
        #   Call
        #     Name(source)
        #       Load
        #     Str
        #     Str
        #     Str # Optional

        # we want to test this node to see if it looks like this, and
        # then extract the source's parameters

        # if first subnode is a name, and == "source"
        # collect the next "Str" arguments that follow
        # store these in a sourcePattern Dictionary member
        state = 0
        source = None
        pattern = None
        instance = None
        for n in ast.iter_child_nodes(node):
            if state == 0:
                if type(n) == ast.Name:
                    for field, value in ast.iter_fields(n):
                        if field == 'id' and value == 'source':
                            state = 1
                            source = None
                            pattern = None
                            instance = None
                            break
                    continue
                break
            elif state == 1:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if field == 's':
                            # source is specified
                            source = value
                            state = 2
                            break
                    continue
                break
            elif state == 2:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if field == 's':
                            pattern = value
                            state = 3
                            break
                    continue
                break
            elif state == 3:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if field == 's':
                            instance = value
                            state = 4
                            break
                    continue
                break
            break

        # instance is optional, if not specified it defaults to None 
        if state == 3:
            instance = None

        # if we properly parsed out a source reference, add it to the
        #   returning dictionary
        if state == 3 or state == 4:
            var_dict.src_pat_inst(self.varDict, source, pattern, instance)

    def __call__(self, tree):
        # visit the specified tree
        self.visit(tree)
        # return the sourcePattern dictionary
        return self.varDict


def get_pattern_src_refs(code):
    """
    Parse a pattern string, throw exception if syntax is invalid.
    return the source to pattern to instance dictionary of the sources
    referenced in the code
    """
    logging.debug("Parsing: " + str(code))
    tree = ast.parse(code)
    # tree_print(tree,0)
    return SourceVisitor()(tree)
    
