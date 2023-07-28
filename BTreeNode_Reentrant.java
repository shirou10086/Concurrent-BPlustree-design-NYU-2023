import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;

class BTreeNode<K extends Comparable<K>, V> {
    private List<K> keys;
    private List<V> values;
    private List<BTreeNode<K, V>> children;
    private BTreeNode<K, V> parent;
    private boolean leafNode;
    private Map<K, V> buffer;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    //don't lock read, only lock write

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
    public void lockRead() {
        lock.readLock().lock();
    }

    public void unlockRead() {
        lock.readLock().unlock();
    }

    public void lockWrite() {
        lock.writeLock().lock();
    }

    public void unlockWrite() {
        lock.writeLock().unlock();
    }

}
