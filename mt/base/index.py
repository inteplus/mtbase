'''Utitilites related to indexing.'''


class PartitionIndices(object):
    '''Parititons a collection of items into groups and returns the list of indices for a given group.

    Parameters
    ----------
    item_cnt : int
        number of items in the collection
    max_part_size : int
        maximum number of items per partition
    policy : {'sequential', 'repeated'}
        policy to partition the collection. Value 'sequential' means the collection is grouped sequentially into `[0, max_part_size), [max_part_size, max_part_size*2), ..., [X, item_cnt)`. Value 'repeated' means the items can be repeated beyond `item_cnt`.
    '''
    def __init__(self, item_cnt, max_part_size, policy='sequential'):

        if not isinstance(item_cnt, int) or item_cnt <= 0:
            raise ValueError("Argument 'item_cnt' must be a positive integer, but {} was provided.".format(item_cnt))
        self.item_cnt = item_cnt
        
        if not isinstance(max_part_size, int) or max_part_size <= 0:
            raise ValueError("Argument 'max_part_size' must be a positive integer, but {} was provided.".format(max_part_size))
        self.max_part_size = max_part_size

        if policy not in ['sequential', 'repeated']:
            raise ValueError("Policy must be either 'sequential' or 'repeated', but {} was provided.".format(policy))
        self.policy = policy

    def part_cnt(self):
        '''Returns the number of partitions, or None if 'repeated' policy was provided.'''
        if self.policy == 'sequential':
            return (self.item_cnt + self.max_part_size - 1) // self.max_part_size
        if self.policy == 'repeated':
            return None

    def get_indices(self, part_id):
        '''Returns the list of indices for a given partition.
        
        Parameters
        ----------
        part_id : int
            zero-based partition index

        Returns
        -------
        list
            list of indices of the partition
        '''
        if not isinstance(part_id, int) or part_id < 0:
            raise ValueError("Argument 'part_id' must be a non-negative integer, but {} was provided.".format(part_id))

        if self.policy == 'sequential':
            start_id = part_id*self.max_part_size
            if start_id >= self.item_cnt:
                raise ValueError("Parition {} for a collection of {} items with sequential partition size {} does not exist.".format(part_id, self.item_cnt, self.max_part_size))
            end_id = min(self.item_cnt, (part_id+1)*self.max_part_size)
            return [x for x in range(start_id, end_id)]

        if policy=='repeated':
            start_id = part_id*self.max_part_size
            end_id = (part_id+1)*self.max_part_size
            return [x%item_cnt for x in range(start_id, end_id)]
