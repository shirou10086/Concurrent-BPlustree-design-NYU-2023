import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BTree<K extends Comparable<K>, V> {
    private BTreeNode<K, V> root;
    private int bufferSize;
    private HashPartition<K, V> partition;

    public BTree(int bufferSize, int numPartitions) {
        this.root = null;
        this.bufferSize = bufferSize;
        this.partition = new HashPartition<>(numPartitions);
    }

    public void insert(K key, V value) {
        root.lockWrite();
        try {
            if (root == null) {
                root = new BTreeNode<>(true);
                root.getKeys().add(key);
                root.getValues().add(value);
                root.setBuffer(new HashMap<>());
                root.getBuffer().put(key, value);
            } else {
                BTreeNode<K, V> node = findLeafNode(root, key);
                insertIntoNode(node, key, value);
                if (node.getKeys().size() > bufferSize) {
                    splitNode(node);
                }
            }
        } finally {
            root.unlockWrite();
        }
    }

    private BTreeNode<K, V> findLeafNode(BTreeNode<K, V> node, K key) {
        if (node.isLeafNode()) {
            return node;
        } else {
            int index = 0;
            while (index < node.getKeys().size() && key.compareTo(node.getKeys().get(index)) >= 0) {
                index++;
            }
            return findLeafNode(node.getChildren().get(index), key);
        }
    }

    private void insertIntoNode(BTreeNode<K, V> node, K key, V value) {
        int index = 0;
        while (index < node.getKeys().size() && key.compareTo(node.getKeys().get(index)) >= 0) {
            index++;
        }
        node.getKeys().add(index, key);
        node.getValues().add(index, value);
        node.getBuffer().put(key, value);

        if (node.getBuffer().size() > bufferSize) {
            writeBufferToDisk();
        }
    }


    private void splitNode(BTreeNode<K, V> node) {
        int midIndex = node.getKeys().size() / 2;
        K midKey = node.getKeys().get(midIndex);
        V midValue = node.getValues().get(midIndex);

        BTreeNode<K, V> leftNode = new BTreeNode<>(node.isLeafNode());
        for (int i = 0; i < midIndex; i++) {
            leftNode.getKeys().add(node.getKeys().get(i));
            leftNode.getValues().add(node.getValues().get(i));
        }
        leftNode.setBuffer(new HashMap<>(leftNode.getBuffer()));

        BTreeNode<K, V> rightNode = new BTreeNode<>(node.isLeafNode());
        for (int i = midIndex + 1; i < node.getKeys().size(); i++) {
            rightNode.getKeys().add(node.getKeys().get(i));
            rightNode.getValues().add(node.getValues().get(i));
        }
        rightNode.setBuffer(new HashMap<>(rightNode.getBuffer()));

        if (!node.isLeafNode()) {
            leftNode.setChildren(new ArrayList<>(node.getChildren().subList(0, midIndex + 1)));
            rightNode.setChildren(new ArrayList<>(node.getChildren().subList(midIndex + 1, node.getChildren().size())));
        }

        if (node.getParent() == null) {
            BTreeNode<K, V> newRoot = new BTreeNode<>(false);
            newRoot.getKeys().add(midKey);
            newRoot.getValues().add(midValue);
            newRoot.getChildren().add(leftNode);
            newRoot.getChildren().add(rightNode);
            root = newRoot;
            leftNode.setParent(newRoot);
            rightNode.setParent(newRoot);
        } else {
            BTreeNode<K, V> parent = node.getParent();
            int index = parent.getChildren().indexOf(node);
            parent.getKeys().add(index, midKey);
            parent.getValues().add(index, midValue);
            parent.getChildren().remove(node);
            parent.getChildren().add(index, leftNode);
            parent.getChildren().add(index + 1, rightNode);
            leftNode.setParent(parent);
            rightNode.setParent(parent);
            parent.getBuffer().put(midKey, midValue);
            if (parent.getKeys().size() > bufferSize) {
                splitNode(parent);
            }
        }
    }

    public V search(K key) {
        root.lockRead();
        try {
            BTreeNode<K, V> node = findLeafNode(root, key);
            int index = node.getKeys().indexOf(key);
            if (index != -1) {
                return node.getValues().get(index);
            } else {
                return null;
            }
        } finally {
            root.unlockRead();
        }
    }


    public void delete(K key) {
        root.lockWrite();
        try {
            if (root == null) {
                return;
            }

            BTreeNode<K, V> node = findLeafNode(root, key);
            int index = node.getKeys().indexOf(key);
            if (index != -1) {
                node.getKeys().remove(index);
                node.getValues().remove(index);
                node.getBuffer().remove(key);
                if (node == root && node.getKeys().isEmpty()) {
                    root = null;
                } else if (node != root && node.getKeys().size() < bufferSize / 2) {
                    borrowOrMerge(node);
                }
            }
        } finally {
            root.unlockWrite();
        }
    }
    private void borrowOrMerge(BTreeNode<K, V> node) {
        BTreeNode<K, V> parent = node.getParent();
        int index = parent.getChildren().indexOf(node);

        if (index > 0 && parent.getChildren().get(index - 1).getKeys().size() > bufferSize / 2) {
            BTreeNode<K, V> sibling = parent.getChildren().get(index - 1);
            K key = sibling.getKeys().remove(sibling.getKeys().size() - 1);
            V value = sibling.getValues().remove(sibling.getValues().size() - 1);
            sibling.getBuffer().remove(key);
            node.getKeys().add(0, key);
            node.getValues().add(0, value);
            node.getBuffer().put(key, value);

            K parentKey = parent.getKeys().get(index - 1);
            V parentValue = parent.getValues().get(index - 1);
            parent.getBuffer().put(parentKey, parentValue);
            parent.getKeys().set(index - 1, sibling.getKeys().get(sibling.getKeys().size() - 1));
            parent.getValues().set(index - 1, sibling.getValues().get(sibling.getValues().size() - 1));
            sibling.getBuffer().put(sibling.getKeys().get(sibling.getKeys().size() - 1),
                    sibling.getValues().get(sibling.getValues().size() - 1));
        } else if (index < parent.getChildren().size() - 1 && parent.getChildren().get(index + 1).getKeys().size() > bufferSize / 2) {
            BTreeNode<K, V> sibling = parent.getChildren().get(index + 1);
            K key = sibling.getKeys().remove(0);
            V value = sibling.getValues().remove(0);
            sibling.getBuffer().remove(key);
            node.getKeys().add(key);
            node.getValues().add(value);
            node.getBuffer().put(key, value);

            K parentKey = parent.getKeys().get(index);
            V parentValue = parent.getValues().get(index);
            parent.getBuffer().put(parentKey, parentValue);
            parent.getKeys().set(index, sibling.getKeys().get(0));
            parent.getValues().set(index, sibling.getValues().get(0));
            sibling.getBuffer().put(sibling.getKeys().get(0), sibling.getValues().get(0));
        } else if (index > 0) {
            BTreeNode<K, V> sibling = parent.getChildren().get(index - 1);
            sibling.getKeys().addAll(node.getKeys());
            sibling.getValues().addAll(node.getValues());
            sibling.getBuffer().putAll(node.getBuffer());
            sibling.getChildren().addAll(node.getChildren());
            for (BTreeNode<K, V> child : node.getChildren()) {
                child.setParent(sibling);
            }
            parent.getKeys().remove(index - 1);
            parent.getValues().remove(index - 1);
            parent.getChildren().remove(index);
            parent.getBuffer().removeAll(node.getBuffer());
            if (parent == root && parent.getKeys().isEmpty()) {
                root = sibling;
                sibling.setParent(null);
            } else if (parent != root && parent.getKeys().size() < bufferSize / 2) {
                borrowOrMerge(parent);
            }
        } else {
            BTreeNode<K, V> sibling = parent.getChildren().get(index + 1);
            node.getKeys().addAll(sibling.getKeys());
            node.getValues().addAll(sibling.getValues());
            node.getBuffer().putAll(sibling.getBuffer());
            node.getChildren().addAll(sibling.getChildren());
            for (BTreeNode<K, V> child : sibling.getChildren()) {
                child.setParent(node);
            }
            parent.getKeys().remove(index);
            parent.getValues().remove(index);
            parent.getChildren().remove(index + 1);
            parent.getBuffer().removeAll(sibling.getBuffer());
            if (parent == root && parent.getKeys().isEmpty()) {
                root = node;
                node.setParent(null);
            } else if (parent != root && parent.getKeys().size() < bufferSize / 2) {
                borrowOrMerge(parent);
            }
        }
    }

    public void loadLevelToBuffer(int level) {
        root.lockWrite();
        try {
            if (root == null) {
                return;
            }

            List<BTreeNode<K, V>> levelNodes = new ArrayList<>();
            collectLevelNodes(root, level, 0, levelNodes);

            for (BTreeNode<K, V> node : levelNodes) {
                loadNodeToBuffer(node);
            }
        } finally {
            root.unlockWrite();
        }
    }


    private void collectLevelNodes(BTreeNode<K, V> currentNode, int targetLevel, int currentLevel,
            List<BTreeNode<K, V>> levelNodes) {
        if (currentLevel == targetLevel) {
            levelNodes.add(currentNode);
        } else {
            if (!currentNode.isLeafNode()) {
                for (BTreeNode<K, V> child : currentNode.getChildren()) {
                    collectLevelNodes(child, targetLevel, currentLevel + 1, levelNodes);
                }
            }
        }
    }

    private void loadNodeToBuffer(BTreeNode<K, V> node) {
        node.setBuffer(new HashMap<>());
        for (int i = 0; i < node.getKeys().size(); i++) {
            K key = node.getKeys().get(i);
            V value = node.getValues().get(i);
            node.getBuffer().put(key, value);
        }
    }

    public void writeBufferToDisk() {
        root.lockWrite();
        try {
            if (root == null) {
                return;
            }

            writeNodeBufferToDisk(root);
        } finally {
            root.unlockWrite();
        }
    }
    private void writeNodeBufferToDisk(BTreeNode<K, V> node) {
        if (!node.isLeafNode()) {
            for (BTreeNode<K, V> child : node.getChildren()) {
                writeNodeBufferToDisk(child);
            }
        }

        for (K key : node.getBuffer().keySet()) {
            V value = node.getBuffer().get(key);
            partition.insert(key, value);
        }

        node.getBuffer().clear();
    }
}
