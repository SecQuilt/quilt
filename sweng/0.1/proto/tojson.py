#!/usr/bin/env python
import json

line = '"timestamp":"Apr  4 13:13:51","content":"jack avahi-daemon[2301]: Service \\\"jack\\\" (/services/ssh.service) successfully established."'

newline = '{' + line + '}'
print 'IN', newline
o = json.loads(newline)
print 'OUT', type(o), str(o)
