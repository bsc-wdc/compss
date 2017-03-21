from pycompss.util.sizer import total_sizeof
from inconsistent_action import InconsistentAction

class CachedObject(object):
    def __init__(self, obj_identifier, obj_size, last_hit_time, obj):
        self.id = obj_identifier
        self.size = obj_size
        self.obj = obj
        self.hits = 1
        self.last_hit_time = last_hit_time

def default_comparison_function(x, y):
    '''
        The default comparison function for the cache object
        It is defined as a module function instead of a lambda
        to avoid serialization problems
    '''
    return x.hits < y.hits if x.hits != y.hits\
            else x.last_hit_time < y.last_hit_time

class Cache(object):
    '''
        Persistent worker cache mockup.
        This is the "core" version without considerations
        about thread safety or efficiency.
    '''

    def __init__(self,
        comparison_function = default_comparison_function,
        size_limit = 200*1024*1024,
        *args,
        **kwargs):
        '''
            Constructor method. It can be called without arguments (see default
            values), but some configuration arguments can be passed:
            - comparison_function: a function
              (CachedObject A, CachedObject B) -> Bool that returns True
              if object A has lower priority than B. Note that lower priority
              means to be erased before.
            - size_limit: The maximum allowed cache size (in bytes). By cache
              size is meant the total amount of bytes occupied by cache objects.
              If the addition of an object leads to a greater cache size than
              the limit, then the object with least priority will be deleted
              until a consistent state is reached or until the cache is empty.
        '''
        self.__comparator = comparison_function
        self.size_limit = size_limit
        self.args = args
        self.kwargs = kwargs
        # let's use a list for our mockup
        self.cache = []
        self.current_time = 0
        self.total_size = 0

    def _enforce_invariant(self):
        while not self.is_empty() and not self.cache_fulfills_invariant():
            to_erase = self.get_last().id
            self.delete(to_erase)

    def _increase_time(self):
        self.current_time += 1

    def cache_fulfills_invariant(self):
        '''
            Tells if the cache fulfills the invariant.
        '''
        return self.total_size <= self.size_limit

    def get_object_size(self, obj):
        '''
            Computes the (approximate size of an object)
        '''
        return total_sizeof(obj)

    def has_object(self, obj_identifier):
        '''
            Checks if an object identifier is present in our cache.
        '''
        return any([x for x in self.cache if x.id == obj_identifier])

    def add(self, obj_identifier, obj, object_size = -1):
        '''
            Add an identified object to the cache.
            If the identifier was already present, a
            InconsistentAction exception will be raised.

            If add is consistent, then self.current_time is increased by
            one.

            @param obj_identifier: The identifier of the object (it's file,
            id, etc...)
            @param obj: The object to store
        '''
        if self.has_object(obj_identifier):
            raise InconsistentAction("%s identifier is already used by\
            object %s(so object %s cannot use it)"%\
            (obj_identifier, obj, self.get(obj_identifier)))
        else:
            if object_size == -1:
                object_size = self.get_object_size(obj)
            if object_size + self.total_size <= self.size_limit:
                self.cache.append(
                    CachedObject(obj_identifier,
                                object_size,
                                self.current_time,
                                obj
                                )
                )
                self.total_size += object_size
            self._increase_time()
            self._enforce_invariant()

    def get(self, obj_identifier):
        '''
            Given an identifier, gets the object associated with this id.
            If the object is not present, then a KeyError will be raised.

            @param obj_identifier: The identifier of the object
        '''
        for obj_wrapper in self.cache:
            if obj_wrapper.id == obj_identifier:
                return obj_wrapper.obj
        raise KeyError("Obj identifier %s is not in cache"%obj_identifier)

    def delete(self, obj_identifier):
        '''
            Deletes an object from our cache. It the object was not present,
            then a KeyError exception will be raised.

            @param obj_identifier: The identifier of the object
        '''
        for obj_wrapper in self.cache:
            if obj_wrapper.id == obj_identifier:
                self.total_size -= obj_wrapper.size
                self.cache.remove(obj_wrapper)
                return
        raise KeyError("Obj identifier %s is not in cache"%obj_identifier)

    def hit(self, obj_identifier):
        '''
            Adds a hit to a given object identifier. It also refreshes (and
            increases) the internal current_time variable, too.
            If there is no object with this id, a KeyError will be thrown.

            @param obj_identifier: The identifier of the object
        '''
        for obj_wrapper in self.cache:
            if obj_wrapper.id == obj_identifier:
                obj_wrapper.hits += 1
                obj_wrapper.last_hit_time = self.current_time
                self._increase_time()
                return
        raise KeyError("Obj identifier %s is not in cache"%obj_identifier)

    def is_empty(self):
        '''
            Checks if the cache is empty
        '''
        return len(self.cache) == 0

    def get_last(self):
        '''
            Gets the last object in terms of priority.
            Returns None if the cache is empty.
            In other words, returns the first object that should be
            deleted.
            As an important remark, this function returns the CachedObject
            wrapper, not the object "per se"
        '''
        if self.is_empty():
            return None
        ret_val = self.cache[0]
        for obj_wrapper in self.cache:
            if self.__comparator(obj_wrapper, ret_val):
                ret_val = obj_wrapper
        return ret_val

    def print_cache_content(self):
        '''
            Prints the cache content.
        '''
        print '-'*40
        print 'CACHE CONTENT'
        for obj_wrapper in self.cache:
            print '-'*20
            print 'OBJ: %s'%obj_wrapper.obj
            print 'ID: %s'%obj_wrapper.id
            print 'HITS: %s'%obj_wrapper.hits
            print 'LAST HIT TIME: %s'%obj_wrapper.last_hit_time
