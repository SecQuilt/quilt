#!/usr/bin/env python
#REVIEW
import logging
import ast

class SourceVisitor(ast.NodeVisitor):
    """
    Provide virtual callbacks for each appropriate node of the 
    AST.  Reposnible for collecting sources referenced in pattern
    code
    """
    def __init__(self):
        # chain call to parent constructor
        ast.NodeVisitor.__init__(self)
        # initialize sourcePattern dictionary member
        self.srcPatDict = {}

    def visit_Call(self, node): 
        # chain call to perent class generic visitor to assure 
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
        for n in ast.iter_child_nodes(node):
            if state == 0:
                if type(n) == ast.Name:
                    for field, value in ast.iter_fields(n):
                        if (field == 'id' and value == 'source'):
                            state = 1
                            sourceDict = None
                            break
                    continue
                break
            elif state == 1:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if (field == 's'):
                            # source is specified
                            if value not in self.srcPatDict:
                                self.srcPatDict[value] = {}
                            sourceDict = self.srcPatDict[value]
                            state = 2
                            break
                    continue
                break
            elif state == 2:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if (field == 's'):
                            if value not in sourceDict:
                                sourceDict[value] = {}
                            patDict = sourceDict[value]
                            state = 3
                            break
                    continue
                break
            elif state == 3:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if (field == 's'):
                            if value not in patDict:
                                patDict[value] = {}
                            state = 4
                            break
                    continue
                break
            break

        # instance is optional, if not specified it defaults to None 
        if state == 3:
            patDict[value][None]={}

    def __call__(self, tree):
        # visit the specified tree
        self.visit(tree)
        # return the sourcePattern dictionary
        return self.srcPatDict

def get_pattern_src_refs(code):
    """
    Parse a pattern string, throw exception if syntax is invalid.
    return the source to pattern to instance dictionary of the sources
    referenced in the code
    """
    logging.debug("Parsing: " + str(code))
    tree = ast.parse(code)
    return SourceVisitor()(tree)
