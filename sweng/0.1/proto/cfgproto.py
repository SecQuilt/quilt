#!/usr/bin/env python

import ConfigParser
import pprint

c = ConfigParser.ConfigParser()
c.read('foo.cfg')

print c.get('section','useof')
print c.get('section','complex')

pprint.pprint (c.items('section'))
pprint.pprint (c._sections['section'])

