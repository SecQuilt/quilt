#!/usr/bin/env python
import logging
import ast
import threading
import quilt_data

class _field:
    def __init__(self, eventSpecsId, fieldName):
        # set id and field name in member data
        self.eventSpecsId = eventSpecsId
        self.fieldName = fieldName

    def __getitem__(self, index): 
        # get the event list from the global event dict with this id
        eventSpecs = _eventPool[self.eventSpecsId]
        eventSpec = eventSpecs[index]
        return eventSpec[self.fieldName]

    def __call__(self, tree):
        # visit the specified tree
        self.visit(tree)
        # return the sourcePattern dictionary
        return self.srcPatDict

    def __setitem__(self,key,val):
        # raise unimplemented exception
        raise Exception("This function is not implemented")

    def __delitem__(self, key):
        # raise unimplemented exception
        raise Exception("This function is not implemented")

    def __len__(self):
        # get the event list from the global event dict with this id
        # return length of event list
        eventSpecs = _eventPool[self.eventSpecsId]
        eventSpec = eventSpecs[index]
        return len(eventSpec)
        
    def __eq__(self, value):
        # construct a new eventID based on the calling context
        # if if this eventID exist in global event dict
            # return those events
        # get the event list from the global event dict with this id
        # create new event list with events that have this
        #   field matching the value
        # set new event list into global events dict
        # return a new _pattern for the new event list
        pass

class _pattern:
    def __init__(self, eventsId, events):
        # set events and id in member data
        pass

    def __getitem__(self, fieldName):
        # return a new _field object from this
        #   eventId, and fieldName
        pass
        
    def __setitem__(self,key,val):
        # raise unimplemented exception
        pass

    def __delitem__(self, key):
        # raise unimplemented exception
        pass

    def __len__(self):
        # get the event list from the global event dict with this id
        # return length of event list
        pass

def at(pattern):
    # return timestamp field of the pattern
    return pattern['timestamp']

def source(srcName,srcPatName=None,srcPatInstance=None):
    """
    Construct a pattern wrapper from the specified source
    """
    # use the global query spec
    srcQuerySpecs = quilt_data.query_spec_get(_
            querySpec,sourceQuerySpecs=True)
    # iterate the srcQueries
    for srcQueryId, srcQuerySpec in srcQuerySpecs.values():
        # if matching (srcName, patName, and patInstanceName)
        # if None was supplied as one of the parameter, then there
        #   must only be one instance to choose, keyed by 'None'
        if (
                srcName == quilt_data.src_query_spec_get(
                    srcQuerySpec, name=True) 
                    and

                srcPatName == quilt_data.src_query_spec_get(
                    srcQuerySpec, srcPatternName=True) 
                    and

                srcPatInstance == quilt_data.src_query_spec_tryget(
                    srcQuerySpec, srcPatternInstance=True) 
            ):

            # get the global srcResults for that srcQueryID
            srcResults = _eventPool[srcQueryId]

            # return a new _pattern with the srcResults as the events
            return _pattern(srcQueryId, srcResults)

    # raise exception if mathing srcQuery not found
    raise Exception("


_interpret_lock = threading.Lock()

def evaluate_query(patternSpec, querySpec, srcResults):
    """
    Semantically process the source results according to the pattern
    code.  Return the results.
    NOTE: Do not call get_pattern_vars when this is on the callstack
    """
    # try to evaluate the query
    try:
        # lock a global lock
        _interpret_lock.lock()
        # set querySpec and srcResults (as eventDict) into global scope
        globals()['_querySpec'] = querySpec
        # we will be adding things to the event pool, this will modify
        # a colleciton that is named only to have source results
        # TODO, figure out if a .copy() would do a deep copy, or think
        #   manual shallow copy
        globals()['_eventPool'] = srcResults

        # evaluate the pattern code
        code = quilt_data.pat_spec_get(patternSpec, code=True)

        # return results
        return eval(code)

    finally:
        # remove querySpec and eventDict from globals scope
        del globals()['_eventPool']
        del globals()['_querySpec']
        # unlock a global lock
        _interpret_lock.unlock()
