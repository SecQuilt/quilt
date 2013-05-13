#!/usr/bin/env python
import ast
import sys
import pprint

class v(ast.NodeVisitor):
    def __init__(self):
        ast.NodeVisitor.__init__(self)
        self.head={}
        self.cur=[self.head]


    def generic_visit(self, node):
        print type(node).__name__

        ast.NodeVisitor.generic_visit(self, node)

    def visit_Name(self, node): 
        print 'Name:', node.id

def visit(node, depth):

    rnode = {}
    basename = type(node).__name__
    name = basename
    index = 0
    while name in rnode:
        index = index + 1
        name = basename + index 



    for n in ast.iter_child_nodes(node):
        subobj = visit(n,depth+1)
        if len(subobj.keys()) == 0:
            continue

        if name not in rnode:
            kids = []
            rnode[name] = kids
        else:
            kids = rnode[name]

        kids.append(subobj)

    for field, value in ast.iter_fields(node):
        if (field == 'id'):
            rnode[field] = value

    return rnode


def tree_print(node, depth):

    basename = type(node).__name__

    s = ""
    for i in range(depth):
        i = i
        s += " | "

    s += basename

    for field, value in ast.iter_fields(node):
        if (field == 'id'):
            s += "(" + value + ")"

    print s
    for n in ast.iter_child_nodes(node):
        tree_print(n,depth+1)

def print_stack(s):
    q = "[ "
    for i in s:
        basename = type(i)
        q += str(basename) + "r, "
    q += " ["
    print q
        
    
def tree_do(node, depth, stack):

    basename = type(node).__name__

    s = ""
    for i in range(depth):
        i = i
        s += " | "

    s += basename

    for field, value in ast.iter_fields(node):
        if (field == 'id'):
            s += "(" + value + ")"

    stack.append(node)
    #print_stack(stack)
    for n in ast.iter_child_nodes(node):
        tree_do(n,depth+1,stack)

    if type(node) == ast.Add:
        arg1 = stack.pop()
        arg2 = stack.pop()
        # print arg1._fields, '+', arg2
    elif type(node) == ast.Load:
        lop = stack.pop()
    elif type(node) == ast.Name:
        n = stack.pop()
        print n._fields


def get_pattern_vars(code):
    """
    Parse a pattern string, throw exception if syntax is invalid.
    return the set of variables mentioned in the pattern
    """
    # use the ast module, ast.walk() iterator
    # parse the pattern string and return set
    tree = ast.parse(code)
    srcs = set()
    for n in ast.walk(tree):
        for field, value in ast.iter_fields(n):
            if (field == 'id'):
                srcs.add(value)
    return srcs

def justwalk(tree):
    srcs = set()

    for n in ast.walk(tree):
        for field, value in ast.iter_fields(n):
            if (field == 'id'):
                src = []

    print srcs
        
class srcpuller(ast.NodeVisitor):


    def visit_Call(self, node): 
        super(srcpuller, self).generic_visit(node)
        state = 0
        for n in ast.iter_child_nodes(node):
            if state == 0:
                if type(n) == ast.Name:
                    for field, value in ast.iter_fields(n):
                        if (field == 'id' and value == 'source'):
                            state = 1
                            break
                    continue
            elif state == 1:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if (field == 's'):
                            print 'sourc name:', value
                            state = 2
                            break
                    continue

            elif state == 2:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if (field == 's'):
                            print 'pat name:', value
                            state = 3
                            break
                    continue
            elif state == 3:
                if type(n) == ast.Str:
                    for field, value in ast.iter_fields(n):
                        if (field == 's'):
                            print 'inst name:', value
                            break
            break

def do_parse(codeline):
    print "Parsing->", codeline
    #visitor = v()
    tree = ast.parse(codeline)

    tree_print(tree,0)
    #tree_do(tree,0,stack)
    # newobj = visit(tree,0)
    #pprint.pprint(newobj)
    #visitor.visit(tree)
    srcpuller().visit(tree)


if __name__ == "__main__":
    codelinex = ' '.join(sys.argv[1:])
    do_parse(codelinex)
0
