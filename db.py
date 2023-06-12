class BTreeNode:
    def __init__(self, t, is_leaf=True):
        self.keys = []
        self.children = []
        self.is_leaf = is_leaf
        self.t = t

    def insert(self, key, t):
        if self.is_leaf:
            self._insert_leaf(key)
        else:
            index = self._find_child_index(key)
            child = self.children[index]
            if len(child.keys) == 2 * t - 1:
                self._split_child(index, t)
                if key > self.keys[index]:
                    index += 1
            child.insert(key, t)

    def _insert_leaf(self, key):
        index = 0
        while index < len(self.keys) and self.keys[index] < key:
            index += 1
        self.keys.insert(index, key)

    def _find_child_index(self, key):
        index = 0
        while index < len(self.keys) and key > self.keys[index]:
            index += 1
        return index

    def _split_child(self, index, t):
        child = self.children[index]
        new_child = BTreeNode(t=t, is_leaf=child.is_leaf)
        self.keys.insert(index, child.keys[t - 1])
        self.children.insert(index + 1, new_child)
        new_child.keys = child.keys[t:]
        child.keys = child.keys[:t - 1]
        if not child.is_leaf:
            new_child.children = child.children[t:]
            child.children = child.children[:t]

    def print_node(self):
        print("Node Keys:", self.keys)
        print("Is Leaf:", self.is_leaf)
        print("Children:")
        for child in self.children:
            print(child.keys)

class BTree:
    def __init__(self, t):
        self.root = BTreeNode(t=t, is_leaf=True)
        self.t = t

    def insert(self, key):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            new_root = BTreeNode(t=self.t, is_leaf=False)
            self.root = new_root
            new_root.children.append(root)
            new_root._split_child(0, self.t)
            new_root.insert(key, self.t)
        else:
            root.insert(key, self.t)

def hash_partition(key, num_partitions):
    hashed_key = hash(key)
    partition_index = hashed_key % num_partitions
    return partition_index

if __name__ == '__main__':
    t = 3  # B-tree parameter: minimum degree
    num_in_memory_levels = 2  # Number of levels to keep in memory
    num_partitions = 3  # Number of disk partitions

    btree = BTree(t)
    keys = [7, 2, 4, 1, 8, 5, 9, 3, 6]
    for key in keys:
        btree.insert(key)

    in_memory_nodes = []
    disk_partitions = {}

    def traverse(node, level):
        if level <= num_in_memory_levels:
            in_memory_nodes.append(node)
        else:
            partition_index = hash_partition(node.keys[0], num_partitions)
            if partition_index not in disk_partitions:
                disk_partitions[partition_index] = []
            disk_partitions[partition_index].append(node)

        if not node.is_leaf:
            for child in node.children:
                traverse(child, level + 1)

    # Perform B-tree insertion

    # Traverse the tree to populate in-memory nodes and disk partitions
    traverse(btree.root, 1)

    # In-memory nodes
    print("In-memory nodes:")
    for node in in_memory_nodes:
        node.print_node()

    # Disk partitions
    print("Disk partitions:")
    for partition_index, nodes in disk_partitions.items():
        print("Partition", partition_index)
        for node in nodes:
            node.print_node()
