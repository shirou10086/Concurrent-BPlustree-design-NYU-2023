import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
