#!/usr/bin/env python
import ast
import sys
# import pprint

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




def do_parse(codeline):
    print "Parsing->", codeline
    #visitor = v()
    tree = ast.parse(codeline)

#    srcs = set()

#   for n in ast.walk(tree):
#       for field, value in ast.iter_fields(n):
#           if (field == 'id'):
#               srcs.add(value)
#   print srcs
        
    stack = []
    tree_print(tree,0)
    tree_do(tree,0,stack)
    #newobj = visit(tree,0)
    #pprint.pprint(newobj)
    #visitor.visit(tree)

if __name__ == "__main__":
    codelinex = ' '.join(sys.argv[1:])
    do_parse(codelinex)

