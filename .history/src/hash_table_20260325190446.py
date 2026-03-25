# hash_table.py
# Authors: Yaxita Amin & Helen Li
# MSML606 HW3 Extra Credit — NYC Taxi Trip Hash Indexer
# Description: Core hash table implementation with chaining for collision resolution.

# ─────────────────────────────────────────────
# SPLIT GUIDE:
#   Yaxita  → Node, HashTable._hash, HashTable.insert, HashTable.get_stats
#   Helen   → HashTable.__init__, HashTable.lookup, HashTable.delete
# ─────────────────────────────────────────────


class Node:
    """
    A single node in a singly-linked list used for chaining.

    Each node stores one key-value pair and a pointer to the next node
    in the same bucket (for collision resolution).

    Attributes:
        key   (str)  : The composite trip key, e.g. "2_2024-03-01 14:35:21"
        value (dict) : Trip metadata (fare, distance, passenger count, etc.)
        next  (Node) : Pointer to the next node in the chain; None if last.
    """

    def __init__(self, key, value):
        # TODO (Yaxita): Store key, value, and set next to None
        self.key=key
        self.value=value
        self.next=None


class HashTable:
    """
    Hash table with separate chaining for collision resolution.

    Designed to index NYC Taxi trip records using a composite key of
    VendorID + tpep_pickup_datetime. Supports O(1) average-case insert
    and lookup even at 1M+ records.

    Attributes:
        size             (int)  : Number of buckets in the table.
        buckets          (list) : List of Node heads, one per bucket.
        total_items      (int)  : Total number of key-value pairs stored.
        collision_count  (int)  : Number of collisions encountered on insert.
    """

    def __init__(self, size=10007):
        """
        Initialize the hash table.

        Args:
            size (int): Number of buckets. Default is 10007 (a prime number
                        chosen to reduce clustering and spread keys evenly).

        TODO (Helen): 
            - Initialize self.size, self.buckets (list of None * size)
            - Set self.total_items = 0 and self.collision_count = 0
        """
        self.size = size
        self.buckets = [None] * size
        self.total_items = 0
        self.collision_count = 0

    def _hash(self, key):
        """
        Map a composite string key to a valid bucket index.

        Uses a polynomial rolling hash: iterates over each character in the
        key, accumulating a weighted sum using a prime base, then takes modulo
        of table size.

        Args:
            key (str): Composite string key, e.g. "2_2024-03-01 14:35:21"

        Returns:
            int: Bucket index in range [0, self.size)

        TODO (Yaxita):
            - Loop over characters in key
            - Use formula: hash_val = (hash_val * BASE + ord(char)) % self.size
            - Suggested BASE = 31 or 37 (both are prime)
            - Return final hash_val
        """
        BASE=31
        hash_val=0
        for char in key:
            hash_val=(hash_val*BASE+ ord(char))% self.size
        return hash_val

    def insert(self, key, value):
        """
        Insert a key-value pair into the hash table.

        If the key already exists, update its value in-place (no duplicate keys).
        If a collision occurs (bucket already occupied), prepend the new node
        to the chain and increment collision_count.

        Args:
            key   (str)  : Composite trip key.
            value (dict) : Trip metadata dictionary.

        TODO (Yaxita):
            - Call self._hash(key) to get index
            - Traverse chain at that bucket to check for duplicate key
            - If duplicate found → update value, return
            - If bucket is empty → insert directly
            - If bucket is occupied (collision) → prepend node, increment collision_count
            - Always increment total_items (unless it was a duplicate update)
        """
        #assuming helen wrote initialize buckets :)
        index=self._hash(key)
        current=self.buckets[index]
        
        #chechking duplicate keys
        while current:
            if current.key==key:
                current.value=value
                return
            current=current.next
        
        #collison case- if bucket exist node
        if self.buckets[index] is not None:
            self.collision_count+=1
            
        #new node to chain
        new_node = Node(key, value)
        new_node.next = self.buckets[index]
        self.buckets[index] = new_node
        self.total_items += 1
        
    def lookup(self, key):
        """
        Retrieve the value associated with a given key.

        Traverses the chain at the hashed bucket index until the key is found
        or the end of the chain is reached.

        Args:
            key (str): Composite trip key to search for.

        Returns:
            dict or None: Trip metadata if found, None if key does not exist.

        TODO (Helen):
            - Hash the key to get bucket index
            - Walk the linked list at that bucket
            - Return node.value when node.key == key
            - Return None if end of chain is reached without a match
        """
        pass

    def delete(self, key):
        """
        Remove a key-value pair from the hash table.

        Handles edge cases: key not found, deletion of head node,
        deletion of a mid-chain node.

        Args:
            key (str): Composite trip key to delete.

        Returns:
            bool: True if deleted successfully, False if key not found.

        TODO (Helen):
            - Hash the key to get bucket index
            - Traverse the chain tracking previous node
            - Rewire pointers to remove the matching node
            - Decrement total_items, return True
            - Return False if key not found
        """
        pass

    def get_stats(self):
        """
        Compute and return hash table performance statistics.

        Useful for analyzing real-world hashing behavior on noisy data.

        Returns:
            dict with keys:
                - 'total_items'     (int)   : Number of records stored
                - 'table_size'      (int)   : Number of buckets
                - 'load_factor'     (float) : total_items / table_size
                - 'collision_count' (int)   : Total collisions on insert
                - 'empty_buckets'   (int)   : Buckets with no entries
                - 'max_chain_len'   (int)   : Length of the longest chain
                - 'avg_chain_len'   (float) : Avg chain length (non-empty buckets only)

        TODO (Yaxita):
            - Compute load_factor = self.total_items / self.size
            - Loop over self.buckets to count empty buckets and chain lengths
            - Track max chain length and sum for average
            - Return all values in a dict
        """
        empty_buckets = 0
        chain_lengths = []
 
        for bucket in self.buckets:
            if bucket is None:
                empty_buckets += 1
            else:
                length = 0
                current = bucket
                while current:
                    length += 1
                    current = current.next
                chain_lengths.append(length)
 
        max_chain_len = max(chain_lengths) if chain_lengths else 0
        avg_chain_len = (sum(chain_lengths) / len(chain_lengths)) if chain_lengths else 0.0
        load_factor = self.total_items / self.size
 
        return {
            "total_items": self.total_items,
            "table_size": self.size,
            "load_factor": round(load_factor, 4),
            "collision_count": self.collision_count,
            "empty_buckets": empty_buckets,
            "max_chain_len": max_chain_len,
            "avg_chain_len": round(avg_chain_len, 4),
        }