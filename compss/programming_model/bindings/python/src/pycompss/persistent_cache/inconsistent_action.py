class InconsistentAction(Exception):
    '''
        Exception that is thrown when actions that whould lead
        to an inconsistent cache state are performed.
        For example, if our cache has a pair (ID_1, OBJ_1)
        then an InconsistentAction would consist to add
        a pair (ID_1, OBJ_2), since an unique ID -> OBJ
        is supposed (not necessarily the other way, though).
    '''
    pass
