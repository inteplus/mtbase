'''Utitilites related to indexing.'''


__all__ = ['IndexGrouping', 'on_list']


class IndexGrouping(object):
    '''Parititons a collection of items into groups and returns the list of indices for each group.

    Parameters
    ----------
    item_cnt : int
        number of items in the collection
    max_group_size : int
        maximum number of items per group
    policy : {'sequential', 'repeated'}
        policy to partition the collection. Value 'sequential' means the collection is grouped sequentially into `[0, max_group_size), [max_group_size, max_group_size*2), ..., [X, item_cnt)`. Value 'repeated' means the items can be repeated beyond `item_cnt`.
    '''
    def __init__(self, item_cnt, max_group_size, policy='sequential'):

        if not isinstance(item_cnt, int) or item_cnt <= 0:
            raise ValueError("Argument 'item_cnt' must be a positive integer, but {} was provided.".format(item_cnt))
        self.item_cnt = item_cnt
        
        if not isinstance(max_group_size, int) or max_group_size <= 0:
            raise ValueError("Argument 'max_group_size' must be a positive integer, but {} was provided.".format(max_group_size))
        self.max_group_size = max_group_size

        if policy not in ['sequential', 'repeated']:
            raise ValueError("Policy must be either 'sequential' or 'repeated', but {} was provided.".format(policy))
        self.policy = policy

    def group_count(self):
        '''Returns the number of groups, or None if 'repeated' policy was provided.'''
        if self.policy == 'sequential':
            return (self.item_cnt + self.max_group_size - 1) // self.max_group_size
        if self.policy == 'repeated':
            return None

    def get_indices(self, group_id):
        '''Returns the list of indices for a given group.
        
        Parameters
        ----------
        group_id : int
            zero-based group index

        Returns
        -------
        list
            list of indices of the group
        '''
        if not isinstance(group_id, int) or group_id < 0:
            raise ValueError("Argument 'group_id' must be a non-negative integer, but {} was provided.".format(group_id))

        if self.policy == 'sequential':
            start_id = group_id*self.max_group_size
            if start_id >= self.item_cnt:
                raise ValueError("Group {} for a collection of {} items with group size {} and sequential policy does not exist.".format(group_id, self.item_cnt, self.max_group_size))
            end_id = min(self.item_cnt, (group_id+1)*self.max_group_size)
            return [x for x in range(start_id, end_id)]

        if policy=='repeated':
            start_id = group_id*self.max_group_size
            end_id = (group_id+1)*self.max_group_size
            return [x%item_cnt for x in range(start_id, end_id)]


def on_list(func):
    '''Turns a function that operates on each element at a time into a function that operates on a colletion of elements at a time. Can be used as a decorator.

    Parameters
    ----------
    func : function
        the function to build upon. Its expected form is `def func(x, *args, **kwargs) -> object`.

    Returns
    -------
    function
        a wrapper function `def func_on_list(list_x, *args, **kwargs) -> list_of_objects` that invokes `func(x, *args, **kwargs)` for each x in list_x.
    '''
    def func_on_list(list_x, *args, **kwargs):
        return [func(x, *args, **kwargs) for x in list_x]
    return func_on_list
