package simpledb.storage;

import com.sun.corba.se.impl.orb.DataCollectorBase;
import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    //每一页的大小
    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    //默认buffer中保存的页数
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
//    private Map<Integer,Page> buffer;
    private LRUCache<PageId,Page> buffer;



    /**
     * Creates a BufferPool that caches up to numPages pages.
     * 后面要改，暂时不支持并发，后期要加锁
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
//        this.buffer = new HashMap<>(numPages);
        this.buffer  = new LRUCache<>(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if (this.buffer.get(pid)==null) {
            // find the right page in DBFiles
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if (buffer.getSize() > numPages) {
                evictPage();
            }
            buffer.put(pid, page);
            return page;
        }
        return this.buffer.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        //注意，insertTuple函数并不会
        List<Page> pages = dbFile.insertTuple(tid, t);
        for(Page page : pages){
            page.markDirty(true,tid);
            buffer.put(page.getId(),page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid,t);
        for(Page page: pages){
            page.markDirty(true,tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head!=tail){
            Page page = head.value;
            if(page!=null && page.isDirty()!=null){
                DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                //记录日志
                try{
                    Database.getLogFile().logWrite(page.isDirty(),page.getBeforeImage(),page);
                    Database.getLogFile().force();

                    dbFile.writePage(page);
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            head = head.next;
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head!=tail){
            PageId key = head.key;
            if(key!=null && key.equals(pid)){
                buffer.remove(head);
                return;
            }
            head = head.next;
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = buffer.get(pid);

        if(page.isDirty()!=null){
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            try{
                Database.getLogFile().logWrite(page.isDirty(),page.getBeforeImage(),page);
                Database.getLogFile().force();
                page.markDirty(false,null);
                dbFile.writePage(page);
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        while(head!=tail){
            Page page = head.value;
            if(page!=null && page.isDirty()!=null&&page.isDirty().equals(tid) ){
                DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                //记录日志
                try{
                    Database.getLogFile().logWrite(page.isDirty(),page.getBeforeImage(),page);
                    Database.getLogFile().force();
                    page.markDirty(false,null);

                    dbFile.writePage(page);
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            head = head.next;
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page page = buffer.getTail().prev.value;
        if(page!=null && page.isDirty()!=null){
            findNotDirty();
        }else{
            //不是脏页没改过，不需要写磁盘
            buffer.discard();
        }
    }

    private void findNotDirty() throws DbException {
        LRUCache<PageId, Page>.DLinkedNode head = buffer.getHead();
        LRUCache<PageId, Page>.DLinkedNode tail = buffer.getTail();
        tail = tail.prev;
        while (head != tail) {
            Page value = tail.value;
            if (value != null && value.isDirty() == null) {
                buffer.remove(tail);
                return;
            }
            tail = tail.prev;
        }
        throw new DbException("no dirty page");
    }

}
