import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BTree<K extends Comparable<K>, V> {
    private BTreeNode<K, V> root;
    private int bufferSize;
    private HashPartition<K, V> partition;

    public BTree(int bufferSize, int numPartitions, String storagePath) {
        this.root = null;
        this.bufferSize = bufferSize;
        this.partition = new HashPartition<>(numPartitions, storagePath);
    }

    public void insert(K key, V value) {
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
        BTreeNode<K, V> node = findLeafNode(root, key);
        int index = node.getKeys().indexOf(key);
        if (index != -1) {
            return node.getValues().get(index);
        } else {
            return null;
        }
    }

    public void delete(K key) {
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
        if (root == null) {
            return;
        }

        List<BTreeNode<K, V>> levelNodes = new ArrayList<>();
        collectLevelNodes(root, level, 0, levelNodes);

        for (BTreeNode<K, V> node : levelNodes) {
            loadNodeToBuffer(node);
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
        if (root == null) {
            return;
        }

        writeNodeBufferToDisk(root);
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

// B树节点
class BTreeNode<K extends Comparable<K>, V> {
    private List<K> keys;
    private List<V> values;
    private List<BTreeNode<K, V>> children;
    private BTreeNode<K, V> parent;
    private boolean leafNode;
    private Map<K, V> buffer; // 节点的缓冲区

    public BTreeNode(boolean leafNode) {
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        this.children = new ArrayList<>();
        this.parent = null;
        this.leafNode = leafNode;
        this.buffer = new HashMap<>();
    }

    public List<K> getKeys() {
        return keys;
    }

    public void setKeys(List<K> keys) {
        this.keys = keys;
    }

    public List<V> getValues() {
        return values;
    }

    public void setValues(List<V> values) {
        this.values = values;
    }

    public List<BTreeNode<K, V>> getChildren() {
        return children;
    }

    public void setChildren(List<BTreeNode<K, V>> children) {
        this.children = children;
    }

    public BTreeNode<K, V> getParent() {
        return parent;
    }

    public void setParent(BTreeNode<K, V> parent) {
        this.parent = parent;
    }

    public boolean isLeafNode() {
        return leafNode;
    }

    public void setLeafNode(boolean leafNode) {
        this.leafNode = leafNode;
    }

    public Map<K, V> getBuffer() {
        return buffer;
    }

    public void setBuffer(Map<K, V> buffer) {
        this.buffer = buffer;
    }
}

// 哈希分区
class HashPartition<K, V> {
    private int numPartitions;
    private List<Map<K, V>> partitions;
    private String storagePath; // 存储路径

    public HashPartition(int numPartitions, String storagePath) {
        this.numPartitions = numPartitions;
        this.storagePath = storagePath;
        this.partitions = new ArrayList<>();

        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new HashMap<>());
        }
    }

    private int getPartitionIndex(K key) {
        return Math.abs(key.hashCode()) % numPartitions;
    }

    public void insert(K key, V value) {
        int partitionIndex = getPartitionIndex(key);
        Map<K, V> partition = partitions.get(partitionIndex);
        partition.put(key, value);
        writeToDisk(partitionIndex, key, value);
    }

    public V get(K key) {
        int partitionIndex = getPartitionIndex(key);
        Map<K, V> partition = partitions.get(partitionIndex);
        V value = partition.get(key);
        if (value == null) {
            value = readFromDisk(partitionIndex, key);
            if (value != null) {
                partition.put(key, value);
            }
        }
        return value;
    }

    private void writeToDisk(int partitionIndex, K key, V value) {
        String partitionFilePath = storagePath + "/partition_" + partitionIndex + ".dat";
        try (FileOutputStream fileOutputStream = new FileOutputStream(partitionFilePath, true);
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(new KeyValuePair<>(key, value));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    private V readFromDisk(int partitionIndex, K key) {
        String partitionFilePath = storagePath + "/partition_" + partitionIndex + ".dat";
        try (FileInputStream fileInputStream = new FileInputStream(partitionFilePath);
             ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            while (true) {
                KeyValuePair<K, V> pair = (KeyValuePair<K, V>) objectInputStream.readObject();
                if (pair != null && pair.getKey().equals(key)) {
                    return pair.getValue();
                }
            }
        } catch (EOFException ignored) {
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static class KeyValuePair<K, V> implements Serializable {
        private K key;
        private V value;

        public KeyValuePair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}

public class Main {
    public static void main(String[] args) {
        // 创建B树
        int bufferSize = 3;
        int numPartitions = 4;
        String storagePath = "data";
        BTree<Integer, String> btree = new BTree<>(bufferSize, numPartitions, storagePath);

        // 插入键值对
        btree.insert(10, "Value 10");
        btree.insert(5, "Value 5");
        btree.insert(3, "Value 3");
        btree.insert(7, "Value 7");
        btree.insert(12, "Value 12");
        btree.insert(11, "Value 11");
        btree.insert(15, "Value 15");
        btree.insert(14, "Value 14");
        btree.insert(17, "Value 17");
        btree.insert(20, "Value 20");

        // 查询键对应的值
        System.out.println("Value for key 7: " + btree.search(7));
        System.out.println("Value for key 15: " + btree.search(15));
        System.out.println("Value for key 3: " + btree.search(3));
        System.out.println("Value for key 8: " + btree.search(8));

        // 删除键值对
        btree.delete(3);
        btree.delete(10);

        // 查询删除后的键对应的值
        System.out.println("Value for key 3: " + btree.search(3));
        System.out.println("Value for key 10: " + btree.search(10));
    }
}
