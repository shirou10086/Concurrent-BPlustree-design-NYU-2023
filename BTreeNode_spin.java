import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class BTreeNode<K extends Comparable<K>, V> {
    private List<K> keys;
    private List<V> values;
    private List<BTreeNode<K, V>> children;
    private BTreeNode<K, V> parent;
    private boolean leafNode;
    private Map<K, V> buffer;

    private AtomicInteger version = new AtomicInteger(0);

    private static final int NODE_LOCKED = 1;

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

    public void lockNode() {
        int currentVersion;
        do {
            currentVersion = version.get();
            if ((currentVersion & NODE_LOCKED) != 0) {
                continue;
            }
        } while (!version.compareAndSet(currentVersion, currentVersion | NODE_LOCKED));
    }
/*    prevent deadlock:

    public boolean lockNode(long timeout, TimeUnit unit) {
        long stopTime = System.nanoTime() + unit.toNanos(timeout);
        int currentVersion;

        do {
            currentVersion = version.get();
            if ((currentVersion & NODE_LOCKED) != 0) {
                if (System.nanoTime() > stopTime) {
                    return false; //if takes too long time, return false, prevent deadlock 
                }
                continue;
            }
        } while (!version.compareAndSet(currentVersion, currentVersion | NODE_LOCKED));

        return true;
    }
*/

    public void unlockNode() {
        int currentVersion;
        do {
            currentVersion = version.get();
        } while (!version.compareAndSet(currentVersion, currentVersion & ~NODE_LOCKED));
    }

    public int stableVersion() {
        int currentVersion;
        do {
            currentVersion = version.get();
        } while ((currentVersion & NODE_LOCKED) != 0);

        return currentVersion;
    }
}
