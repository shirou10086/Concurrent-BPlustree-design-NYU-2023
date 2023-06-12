class BTreeNode:
    def __init__(self, is_leaf=True):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        self.buffer = []

    def add_to_buffer(self, key):
        self.buffer.append(key)

    def flush_buffer(self):
        for key in self.buffer:
            self.insert_non_full(key)
        self.buffer = []

    def insert_non_full(self, key):
        i = len(self.keys) - 1
        if self.is_leaf:
            self.keys.append(None)
            while i >= 0 and key < self.keys[i]:
                self.keys[i + 1] = self.keys[i]
                i -= 1
            self.keys[i + 1] = key
        else:
            while i >= 0 and key < self.keys[i]:
                i -= 1
            i += 1
            if len(self.keys) == (2 * t) - 1:  # 如果当前节点的键已满，将键添加到缓冲区
                self.add_to_buffer(key)
                if len(self.buffer) == (2 * t) - 1:  # 如果缓冲区满了，刷新缓冲区并进行节点分裂
                    self.flush_buffer()
                    self.split_child(i)
            else:
                if len(self.children[i].keys) == (2 * t) - 1:  # 如果子节点的键已满，先刷新子节点的缓冲区并进行节点分裂
                    self.children[i].flush_buffer()
                    self.split_child(i)
                self.children[i].insert_non_full(key)  # 继续在子节点中插入键

    def split_child(self, i):
        t = B_tree.t
        child = self.children[i]
        new_child = BTreeNode(child.is_leaf)
        self.children.insert(i + 1, new_child)
        self.keys.insert(i, child.keys[t - 1])

        new_child.keys = child.keys[t:(2 * t) - 1]
        child.keys = child.keys[0:t - 1]

        if not child.is_leaf:
            new_child.children = child.children[t:2 * t]
            child.children = child.children[0:t]



class BTree:
    def __init__(self, t, num_partitions):
        self.root = BTreeNode(True)
        self.t = t
        self.num_partitions = num_partitions
        self.simulated_disk = [{} for _ in range(num_partitions)]

    def _simple_hash(self, key):
        return sum([ord(char) for char in str(key)]) % self.num_partitions

    def traverse(self, node=None, level=1, max_level=2):
        if node is None:
            node = self.root

        if level <= max_level:
            print('Level:', level, 'Keys:', node.keys)

            for child in node.children:
                self.traverse(child, level + 1, max_level)
        else:
            hash_val = self._simple_hash(node.keys[0])
            print(f'Hashed partition for level {level}: {hash_val}, Keys: {node.keys}')
            # Simulate storing in disk by putting it in a dictionary
            self.simulated_disk[hash_val][level] = node.keys

    def insert(self, key):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            temp = BTreeNode()
            self.root = temp
            temp.children.insert(0, root)
            temp.insert_non_full(key)
            temp.flush_buffer()
            self.split_child(temp, 0)
        else:
            self.insert_non_full(root, key)

    def insert_non_full(self, node, key):
        i = len(node.keys) - 1
        if node.is_leaf:
            node.keys.append(None)
            while i >= 0 and key < node.keys[i]:
                node.keys[i + 1] = node.keys[i]
                i -= 1
            node.keys[i + 1] = key
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            if len(node.children[i].keys) == (2 * self.t) - 1:
                node.children[i].add_to_buffer(key)
                self.insert_non_full(node.children[i], key)  # 继续插入键到节点中
                if len(node.children[i].buffer) == (2 * self.t) - 1:
                    node.children[i].flush_buffer()
                    self.split_child(node, i)
            else:
                self.insert_non_full(node.children[i], key)

    def split_child(self, node, i):
        t = self.t
        child = node.children[i]
        new_child = BTreeNode(child.is_leaf)
        node.children.insert(i + 1, new_child)
        node.keys.insert(i, child.keys[t - 1])

        new_child.keys = child.keys[t:(2 * t) - 1]
        child.keys = child.keys[0:t - 1]

        if not child.is_leaf:
            new_child.children = child.children[t:2 * t]
            child.children = child.children[0:t]
    '''
    def print_buffered_keys(self):
        print("Buffered keys in BTree:")
        self._print_buffered_keys(self.root)

    def _print_buffered_keys(self, node):
        if node is None:
            return

        if node.buffer:
            print(f"Node keys: {node.keys}, Buffered keys: {node.buffer}")

        if not node.is_leaf:
            for child in node.children:
                self._print_buffered_keys(child)
    '''


B_tree = BTree(3, 4)

keys = [10, 20, 5, 6, 12, 30, 7, 17]
for key in keys:
    B_tree.insert(key)
B_tree.traverse(max_level=2)
