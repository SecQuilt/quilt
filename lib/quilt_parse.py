#!/usr/bin/env python
#REVIEW
import logging
import ast

def get_pattern_vars(code):
    """
    Parse a pattern string, throw exception if syntax is invalid.
    return the set of variables mentioned in the pattern
    """
    # use the ast module, ast.walk() iterator
    # parse the pattern string and return set
    tree = ast.parse(code)
    logging.debug("Parsing: " + str(code))
    srcs = set()
    for n in ast.walk(tree):
        for field, value in ast.iter_fields(n):
            if (field == 'id'):
                srcs.add(value)
    return srcs
