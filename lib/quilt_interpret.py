#!/usr/bin/env python
import threading
import quilt_data
import itertools
# import logging


class _rec:
    def __init__(self, eventsId, index, fieldName):

        # set id and field name in member data
        self.eventsId = eventsId
        # if not _has_events(eventsId):
        #     raise Exception("Record has no parent event list")
        self.fieldName = fieldName
        self.index = index
        # logging.debug('accessing:' + str(self.eventsId) +":"+ str(self.index) +":"+ str(self.fieldName))

        # ISSUE014:  Big mystery here.  If I don't call GetRec or dereference
        #   the event list then an
        #   infinate loop shows up, I can't figure it out
        eventSpecs = _get_events(self.eventsId)
        eventSpecs[self.index]


    def GetRec(self):
        """
        retrieve the wrapped record
        """
        # get the event list from the global event dict with this id
        eventSpecs = _get_events(self.eventsId)
        # get the n'th event from the list
        eventSpec = eventSpecs[self.index]
        # return the event at the appropriate field

        return eventSpec[self.fieldName]

    def __str__(self):
        return "<"+str(self.GetRec())+">"

   
class _field:
    def __init__(self, eventsId, fieldName):
        # set id and field name in member data
        self.eventsId = eventsId
        self.fieldName = fieldName

    def __getitem__(self, index): 
        # create and return a wrapper for this record
        return _rec(self.eventsId, index, self.fieldName)

    def __setitem__(self,key,val):
        # raise unimplemented exception
        raise Exception("This function is not implemented")

    def __delitem__(self, key):
        # raise unimplemented exception
        raise Exception("This function is not implemented")

    def __len__(self):
        # get the event list from the global event dict with this id
        # return length of event list
        eventSpecs = _get_events(self.eventsId)
        return len(eventSpecs)
        
    def __eq__(self, value):
        # construct a new eventID based on the calling context
        # NOTE, we assume RHS is a literal currently
        returnEventsId = (self.eventsId + "." + self.fieldName + 
                "==" + str(value))

        # if if eventID exist in global event dict
        if _has_events(returnEventsId):
            # return those events
            return _pattern(returnEventsId)

        # get the event list from the global event dict with this id
        events = _get_events(self.eventsId)


        # create new event list with events that have this
        #   field matching the value
        returnEvents = [e for e in events if e[self.fieldName] == value]

        # logging.debug("RETUR   " + str(returnEvents))
        # set new event list into global events dict
        # return a new _pattern for the new event list
        return _pattern(returnEventsId, returnEvents)

    def __str__(self):
        s = self.eventsId + '.' + self.fieldName + ' ['
        for i in self:
            s += str(i) + ','
        return s.rstrip(',') + ']'


class _pattern:
    def __init__(self, eventsId, events = None):
        # set events and id in member data
        self.eventsId = eventsId
        # if a new list of events is input, store them in
        #   the global pool
        if events != None:
            _set_events(eventsId,events)

        if not _has_events(eventsId):
            raise Exception("No event list available for: " + str(eventsId))

    def __getitem__(self, fieldName):
        # return a new _field object from this
        #   eventId, and fieldName
        return _field(self.eventsId, fieldName)
        
    def __setitem__(self,key,val):
        # raise unimplemented exception
        raise Exception("This function is not implemented")

    def __delitem__(self, key):
        # raise unimplemented exception
        raise Exception("This function is not implemented")

    def __len__(self):
        # get the event list from the global event dict with this id
        # return length of event list
        return len(_get_events(self.eventsId))

    def __str__(self):
        s = self.eventsId + ":\n"
        for e in self:
            s += "\t" + str(e) + "\n"
        return s
            

def at(pattern):
    # return timestamp field of the pattern
    return pattern['timestamp']

def source(srcName,srcPatName,srcPatInstance=None):
    """
    Construct a pattern wrapper from the specified source
    """
    # use the global query spec
    srcQuerySpecs = quilt_data.query_spec_get(_get_query_spec(), 
            sourceQuerySpecs=True)
    # logging.debug("srcQuerySpecs:\n" + pprint.pformat(srcQuerySpecs))

    # logging.debug("Looking for Source: " + str(srcName) + ", pattern: " + str(srcPatName) 
    #         + ". instance: " + str(srcPatInstance) )


    # iterate the srcQueries
    for srcQueryId, srcQuerySpec in srcQuerySpecs.items():
        # if matching (srcName, patName, and patInstanceName)
        # if None was supplied as one of the parameter, then there
        #   must only be one instance to choose, keyed by 'None'
        curSrc = quilt_data.src_query_spec_get(srcQuerySpec, 
                source=True) 
        curSrcPat = quilt_data.src_query_spec_get( srcQuerySpec, 
                srcPatternName=True) 
        curSrcPatInst = quilt_data.src_query_spec_tryget(
                    srcQuerySpec, srcPatternInstance=True) 

        # logging.debug("checking " + curSrc + ", " + curSrcPat + 
        #         ", " + str( curSrcPatInst))

        if (srcName == curSrc and 
            srcPatName == curSrcPat and 
            srcPatInstance == curSrcPatInst):

            # get the global srcResults for that srcQueryID
            srcResults = _get_events(srcQueryId)

            # return a new _pattern with the srcResults as the events
            return _pattern(srcQueryId, srcResults)

    # raise exception if mathing srcQuery not found
    raise Exception("Source: " + str(srcName) + ", pattern: " + str(srcPatName) 
            + ". instance: " + str(srcPatInstance) +  
            ", could not be found among the source queries")


# http://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
def _check_equal(iterator):
    try:
        iterator = iter(iterator)
        first = next(iterator)
        first = first.GetRec()
        return all(first == rest.GetRec() for rest in iterator)
    except StopIteration:
        return True
    #TODO ISSUE015 ISSUE016

def concurrent(*patterns):
    """return all patterns that occur at the same time"""

    # generate name based on execution context
    returnEventsId = "concurrent("
    for curPattern in patterns:
        returnEventsId += curPattern.eventsId + ","
    returnEventsId = returnEventsId.rstrip(',') +')'

    # if eventlist of generated name exists
    if _has_events(returnEventsId):
        #   return previously generated event list
        return _pattern(returnEventsId)

    # construct a new empty list of fields
    # itterate the patterns
        # call 'at' on the pattern, append to list of fields
    fields = [at(curPattern) for curPattern in patterns]

    # for f in fields:
    #     logging.debug("CONCURRR Fields " + str(f))

    # join all of the field lists using an equality test
    joined = itertools.ifilter(_check_equal, itertools.product(*fields))

    # generate new event list only containing events with timestamps
    #   found during the join
    returnEvents = []
    for joinedTuple in joined:
        # logging.debug("CONCURRR Joined  " + str(type(timestampRecord)))
        # logging.debug("CONCURRR Joined0 " + str(timestampRecord[0]))
        # logging.debug("CONCURRR Joined1 " + str(timestampRecord[1]))

        for timestampRecord in joinedTuple:
            index = timestampRecord.index
            eventsId = timestampRecord.eventsId
            event = _get_events(eventsId)[index] 
            if not event in returnEvents:
                returnEvents.append(event)

    # logging.debug("CONCURRR " + str(returnEvents))
    # record new event list in event dict with this name
    # by returning a _pattern for the new list
    return _pattern(returnEventsId, returnEvents)

    
class _check_follows:
    """
    Object represents a functor.  In the class constructor the delta time
    ammount is passed.  In the class's check funciton is used as a
    callback for pairs of timestamps. 
    """
    def __init__(self, howlong):
        # set how long into member data
        self.howlong = howlong


    def check(self, pair):
        """
        called to check each tuple created from the cartesian product of
        before and after events given to the follows function.  
        pair is a two element tuple (before record, after record). Returns
        True if second element occurs within howlong of the first element.
        """
        # set before record to first value of tuple
        before = pair[0]
        # set after record to second value of tuple
        after = pair[1]
        # set before and after to the value wraped in the record wrapper
        before = before.GetRec()
        after = after.GetRec()
        # return true if before is within howlong of after and is not zero
        #   otherwise return False 
        delta = after - before
        return delta > 0 and delta <= self.howlong
        
        #TODO ISSUE016


        

def follows(howlong, before, after):
    """
    Determine and return the events in after that occur within howlong of
    before.  This does not include events in before and after that occur
    at the same time.
    howlong : time delta
    before : pattern
    after: pattern
    """

    # generate a name for a new pattern like:
    #   follows( howlong, name of before patter, name of after pattern)
    returnEventsId = ("follows(" + str(howlong) + "," + before.eventsId + 
        "," + after.eventsId + ')')
    
    # if events can be found with the generated name
    if _has_events(returnEventsId):
        # return previously generated event list
        return _pattern(returnEventsId)

    # get time fields of before, after by calling at() method
    beforeFields = at(before)
    afterFields = at(after)

    # create a _check_follows functor object
    #   initialize with howlong
    checkFollows = _check_follows(howlong)

    # use itertools iproduct on the fields, and pass them to an ifilter
    #   use _check_follows.check() for filter predicate
    sequenced = itertools.ifilter(checkFollows.check, itertools.product(
        beforeFields, afterFields))

    # create an empty list for returning events
    returnEvents = []
    # use returned iterator from ifilter, for each of the tuples remaining
    #   in the filtered product
    for sequencePair in sequenced:
        # get the second (After) element of the tuple
        afterRecord = sequencePair[1]
        # get the index of the timestampRecord 
        index = afterRecord.index
        # get the name of the events list
        eventsId = afterRecord.eventsId
        # get the event in the named list at the specified index
        event = _get_events(eventsId)[index]
        # if event is not already in the list
        if not event in returnEvents:
            # append the event to a returning list of events 
            returnEvents.append(event)
            
    # return a new pattern wrapper with the generated name and the
    #   newly determined events
    return _pattern(returnEventsId, returnEvents)


def until(before, after):
    """
    Determine all of the events in before that occured prior to any event
    in after.  This includes an event in before that occurs at the same
    time as the first event in after
    """

    # generate a name for a new pattern like:
    #   until( name of before pattern, name of after pattern)
    
    returnEventsId = "until(" + before.eventsId + "," + after.eventsId + ')'
    
    # if events can be found with the generated name
    if _has_events(returnEventsId):
        # return previously generated event list
        return _pattern(returnEventsId)

    # get fields of before, after by calling at() method
    beforeFields = at(before)
    afterFields = at(after)

    # iterate the after timestamp fields
    t = None
    tv = None
    for f in afterFields:
        # find the earliest occuring time
        if t == None or f.GetRec() < tv:
            t = f
            tv = t.GetRec()

    # logging.debug("%%%%%% Earlies time in " + after.eventsId + " is " + str(tv))

    #  empty list for returning events
    returnEvents = []

    if t != None:
        eventsId = before.eventsId
        # iterate the before timestamp fields
        for b in beforeFields:
            # if current timestamp if before or equal to earliest after
            #   timestamp
            if b.GetRec() <= tv:
                # get the index of the timestampRecord event
                index = b.index
                # get the event in the named list at the specified index
                e = _get_events(eventsId)[index]
                # append the event to a returning list of events 
                returnEvents.append(e)

    # return a new pattern wrapper with the generated name and the
    #   newly determined events
    return _pattern(returnEventsId, returnEvents)

def _get_events(eventsId):
#   logging.debug("accessing " + str(eventsId))
    return globals()['_eventPool'][eventsId]

def _set_events(eventsId,events):
    globals()['_eventPool'][eventsId] = events

def _has_events(eventsId):
    # logging.debug("Looking for " + str(eventsId) + " in " + str(globals()['_eventPool'].keys()))
    return eventsId in globals()['_eventPool']

def _get_query_spec():
    return globals()['_querySpec']

_interpret_lock = threading.Lock()

def evaluate_query(patternSpec, querySpec, srcResults):
    """
    Semantically process the source results according to the pattern
    code.  Return the results.
    NOTE: This funciton is non rentrant.
        Do not call evaluate_query when this is on the callstack
    NOTE: The srcResults collection may be modified after calling
    """
    # try to evaluate the query
    with _interpret_lock:
        try:
            # set querySpec and srcResults (as eventDict) into global scope
            globals()['_querySpec'] = querySpec
            # we will be adding things to the event pool, this will modify
            # a colleciton that is named only to have source results, so we
            # preform a rename here.  The calling context will not use it
            # anyway. See NOTE in docstring
            # TODO, figure out if a .copy() would do a deep copy, or think
            #   manual shallow 
            globals()['_eventPool'] = srcResults

            # evaluate the pattern code
            code = quilt_data.pat_spec_get(patternSpec, code=True)

            retpattern = eval(code)

            retobj = _get_events(retpattern.eventsId)

            # logging.debug ("Rsults of interpret are:\n" + 
            # str(type(retobj)) + "\n" + str(dir(retobj)) +"\n" + 
            # pprint.pformat(retobj))

            # return results
            return retobj

        finally:
            # remove querySpec and eventDict from globals scope
            if '_eventPool' in globals():
                del globals()['_eventPool']
            if '_querySpec' in globals():
                del globals()['_querySpec']
