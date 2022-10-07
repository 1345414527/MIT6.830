package simpledb.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 自己实现的一个LRU cache,主要就是map+链表实现
 */
public class LRUCache<K,V> {

    class DLinkedNode {
        K key;
        V value;
        DLinkedNode prev;
        DLinkedNode next;
        public DLinkedNode() {}
        public DLinkedNode(K _key, V _value) {key = _key; value = _value;}
    }

    private Map<K,DLinkedNode> cache = new ConcurrentHashMap<K,DLinkedNode>();
    private int size;
    private int capacity;
    private DLinkedNode head, tail;

    public LRUCache(int capacity){
        this.size = 0;
        this.capacity = capacity;
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head.next = tail;
        head.prev = tail;
        tail.prev = head;
        tail.next = head;
    }

    public int getSize() {
        return size;
    }

    public DLinkedNode getHead(){
        return head;
    }

    public DLinkedNode getTail(){
        return tail;
    }

    public Map<K,DLinkedNode> getCache(){
        return cache;
    }

    /**
     * 接下来的就是主要的逻辑了
     */

    /**
     * 根据key获取元素
     * @param key
     * @return
     */
    public synchronized V get(K key){
        DLinkedNode node = cache.get(key);
        if(node==null){
            return null;
        }
        //如果存在，移到头部
        moveToHead(node);
        return node.value;
    }

    /**
     * 新增元素，注意容量限制
     * @param key
     * @param value
     */
    public synchronized void put(K key,V value){
        DLinkedNode node = this.cache.get(key);
        if(node==null){
            //新增
            DLinkedNode newNode = new DLinkedNode(key,value);
            this.cache.put(key,newNode);
            addToHead(newNode);
            this.size++;
            //判断是否达到上限
            if(this.size>this.capacity){
                //删掉最后一个元素
                DLinkedNode tmp = tail.prev;
                //先在链表中移除
                removeNode(tmp);
                //然后在map中移除
                this.cache.remove(tmp.key);
                this.size--;
            }
        }else{
            //修改
            node.value = value;
            moveToHead(node);
        }
    }



    public void moveToHead(DLinkedNode node){
        removeNode(node);
        addToHead(node);
    }

    public void removeNode(DLinkedNode node){
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    public void remove(DLinkedNode node){
        removeNode(node);
        cache.remove(node.key);
        size--;
    }

    public void addToHead(DLinkedNode node){
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    public synchronized void discard(){
        // 如果超出容量，删除双向链表的尾部节点
        DLinkedNode tail = removeTail();
        // 删除哈希表中对应的项
        cache.remove(tail.key);
        size--;

    }

    private DLinkedNode removeTail() {
        DLinkedNode res = tail.prev;
        removeNode(res);
        return res;
    }













}




