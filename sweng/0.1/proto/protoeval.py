#!/usr/bin/env python
import ast
import sys
import pprint

def tree_print(node, depth):

    basename = type(node).__name__

    if basename != "Load":

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

def until(a,b):
    return a < b

def at(x):
    return x['timestamp']

def concurrent(af,bf):
    a = af.items()
    b = bf.items()
    r = []
    bi = 0
    aadd=[]
    badd=[]
    for curb in b:
        ai = 0
        for cura in a:
            if cura == curb:
                if ai not in aadd:
                    r.append(getEvents(af.name)[ai])
                    aadd.append(ai)
                if bi not in badd:
                    r.append(getEvents(bf.name)[bi])
                    badd.append(bi)
                # print aadd, badd
            ai = ai + 1
        bi = bi + 1

    name = "concurrent(" +  str(af.name) + "," + str(bf.name) + ")"
    return wrapper(name,r)

def follows(dt,af,bf):
    a = af.items()
    b = bf.items()
    r = []
    bi = 0
    aadd=[]
    badd=[]
    for curb in b:
        ai = 0
        for cura in a:
            delta = curb-cura
            if delta >= 0 and delta <= dt:
                if ai not in aadd:
                    r.append(getEvents(af.name)[ai])
                    aadd.append(ai)
                if bi not in badd:
                    r.append(getEvents(bf.name)[bi])
                    badd.append(bi)
                # print aadd, badd
            ai = ai + 1
        bi = bi + 1

    name = "follows(" + str(dt) + "," + str(af.name) + "," + str(bf.name) + ")"
    return wrapper(name,r)


shutdownevents = [
            { 'timestamp' : 100,
                'content'   : 'rsyslog has shutdown' },
            { 'timestamp' : 200,
                'content'   : 'rsyslog has shutdown' },
            { 'timestamp' : 300,
                'content'   : 'rsyslog has shutdown' }]

startupevents = [
            { 'timestamp' : 101,
                'content'   : 'rsyslog has startup' },
            { 'timestamp' : 202,
                'content'   : 'rsyslog has startup' },
            { 'timestamp' : 301,
                'content'   : 'rsyslog has startup' }]

maintevents = [
            { 'timestamp' : 150,
                'content'   : 'running maintenance' },
            { 'timestamp' : 175,
                'content'   : 'running maintenance' },
            { 'timestamp' : 250,
                'content'   : 'running maintenance' },
            { 'timestamp' : 275,
                'content'   : 'running maintenance' },
            { 'timestamp' : 350,
                'content'   : 'running maintenance' },
            { 'timestamp' : 375,
                'content'   : 'running maintenance' }]

bakevents = [
            { 'timestamp' : 150,
                'content'   : 'running bak' },
            { 'timestamp' : 175,
                'content'   : 'running bak' },
            { 'timestamp' : 250,
                'content'   : 'running bak' },
            { 'timestamp' : 350,
                'content'   : 'running bak' },
            { 'timestamp' : 375,
                'content'   : 'running bak' }]

bakflowevents = [
            { 'timestamp' : 151,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 176,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 250,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 275,
                'sip'   : '10.0.0.4', 'dip' :   '74.1.0.1' },
            { 'timestamp' : 351,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 376,
                'sip'   : '10.0.0.4', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 151,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 176,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 250,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 275,
                'sip'   : '10.0.0.5', 'dip' :   '74.1.0.1' },
            { 'timestamp' : 351,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' },
            { 'timestamp' : 376,
                'sip'   : '10.0.0.5', 'dip' :   '10.1.0.1' }
            ]

events = {
    'shutdown' : shutdownevents,
    'startup' : startupevents,
    'maint' : maintevents,
    'bak' : bakevents,
    'bakflow' : bakflowevents
    }


def getEvents(name):
    return events[name]

class fieldwrapper:
    def __init__(self, name, key):
        self.name = name
        self.key = key

    def items(self):
        myevents = getEvents(self.name)
        return [i[self.key] for i in myevents]

    def __getitem__(self, key):
        return self.items()[key]

    def __ne__(self, rhs):
        myevents = getEvents(self.name)
        newe = [i for i in myevents if i[self.key] != rhs]
        name = self.name + "[" + self.key + "]==" + str(rhs)
        
        return wrapper(name,newe)
    def __eq__(self, rhs):
        myevents = getEvents(self.name)
        newe = [i for i in myevents if i[self.key] == rhs]
        name = self.name + "[" + self.key + "]==" + str(rhs)
        
        return wrapper(name,newe)

class wrapper:
    def __init__(self, name, eventString=None):
        self.name = name
        if eventString != None:
            events[name] = eventString

    def __getitem__(self, key):
        print "Accesing", self.name, "[",key,"]"
        return fieldwrapper(self.name, key)

    def __delitem__(self, key):
        pass

    def __len__(self):
        pass



    def __name__(self):
        return self.name

def gen_var(name):
    return wrapper(name)

def do_parse(codeline):

    print "Parsing->", codeline
    #visitor = v()
    tree = ast.parse(codeline)
    tree_print(tree,0)

    srcs = set()

    for n in ast.walk(tree):
        for field, value in ast.iter_fields(n):
            if (field == 'id'):
                srcs.add(value)
    print srcs

    for s in srcs:
        if s in locals() or s in globals():
            continue
        else:
            v = gen_var(s)
            locals()[s] = v


#    r = exec codeline in locals(), globals()
    r = eval( codeline )

    print "\n","RESULTS","\n"
    pprint.pprint(r)
    pprint.pprint(r.name)
    pprint.pprint(getEvents(r.name))

        

if __name__ == "__main__":
    codelinex = ' '.join(sys.argv[1:])
    do_parse(codelinex)

