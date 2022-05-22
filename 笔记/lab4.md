[toc]

# Exercise 1 + 2 + 5

## BufferPoll

### 参数

- private LockManager lockManager;	// 锁的管理器，管理已经持有的锁

  - PageLock：页面锁，某个事务持有的锁（两种类型，共享锁和排他锁）

  ```java
  // 锁
  class PageLock{
      private static final int SHARE = 0;
      private static final int EXCLUSIVE = 1;
      private TransactionId tid;
      private int type;
      public PageLock(TransactionId tid, int type){
          this.tid = tid;
          this.type = type;
      }
      public TransactionId getTid(){
          return tid;
      }
      public int getType(){
          return type;
      }
      public void setType(int type){
          this.type = type;
      }
  }
  ```

  - LockManager：锁管理器

  ```java
  class LockManager {
      ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap = new ConcurrentHashMap<>();
      /**
           * 获取锁
           */
      public synchronized boolean acquiredLock(PageId pageId, TransactionId tid, int requiredType) {
          // 判断当前页是否当前有锁
          if (lockMap.get(pageId) == null) {
              // 创建锁
              PageLock pageLock = new PageLock(tid, requiredType);
              ConcurrentHashMap<TransactionId, PageLock> pageLocks = new ConcurrentHashMap<>();
              pageLocks.put(tid, pageLock);
              lockMap.put(pageId, pageLocks);
              return true;
          }
          // 获取当前锁的等待队列
          ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pageId);
          // tid 没有 Page 上的锁
          if (pageLocks.get(tid) == null) {
              // 如果 当前页已经有锁
              if (pageLocks.size() > 1) {
                  // 如果是共享锁，新增获取对象
                  if (requiredType == PageLock.SHARE) {
                      // tid 请求锁
                      PageLock pageLock = new PageLock(tid, PageLock.SHARE);
                      pageLocks.put(tid, pageLock);
                      lockMap.put(pageId, pageLocks);
                      return true;
                  }
                  // 如果是排他锁
                  else if (requiredType == PageLock.EXCLUSIVE) {
                      // tid 需要获取写锁，拒绝
                      return false;
                  }
              }
              if (pageLocks.size() == 1) {
                  // page 上有一个其他事务， 可能是写锁，也可能是读锁
                  PageLock curLock = null;
                  for (PageLock lock : pageLocks.values()) {
                      curLock = lock;
                  }
                  if (curLock.getType() == PageLock.SHARE) {
                      // 如果请求的锁也是读锁
                      if (requiredType == PageLock.SHARE) {
                          // tid 请求锁
                          PageLock pageLock = new PageLock(tid, PageLock.SHARE);
                          pageLocks.put(tid, pageLock);
                          lockMap.put(pageId, pageLocks);
                          return true;
                      }
                      // 如果是独占锁
                      else if (requiredType == PageLock.EXCLUSIVE) {
                          // tid 需要获取写锁，拒绝
                          return false;
                      }
                  }
                  // 如果是独占锁
                  else if (curLock.getType() == PageLock.EXCLUSIVE) {
                      // tid 需要获取写锁，拒绝
                      return false;
                  }
              }
          }
          // 当前事务持有 Page 锁
          else if (pageLocks.get(tid) != null) {
              PageLock pageLock = pageLocks.get(tid);
              // 事务上是写锁
              if (pageLock.getType() == PageLock.SHARE) {
                  // 新请求的是读锁
                  if (requiredType == PageLock.SHARE) {
                      return true;
                  }
                  // 新请求是写锁
                  else if (requiredType == PageLock.EXCLUSIVE) {
                      // 如果该页面上只有一个锁，就是该事务的读锁
                      if (pageLocks.size() == 1) {
                          // 进行锁升级(升级为写锁)
                          pageLock.setType(PageLock.EXCLUSIVE);
                          pageLocks.put(tid, pageLock);
                          return true;
                      }
                      // 大于一个锁，说明有其他事务有共享锁
                      else if (pageLocks.size() > 1) {
                          // 不能进行锁的升级
                          return false;
                      }
                  }
              }
              // 事务上是写锁
              // 无论请求的是读锁还是写锁，都可以直接返回获取
              return pageLock.getType() == PageLock.EXCLUSIVE;
          }
          return false;
      }
  
      /**
           * 释放锁
           */
      public synchronized boolean releaseLock(TransactionId tid, PageId pageId) {
          // 判断是否持有锁
          if (isHoldLock(tid, pageId)) {
              ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
              locks.remove(tid);
              if (locks.size() == 0) {
                  lockMap.remove(pageId);
              }
              return true;
          }
          return false;
      }
  
      /**
           * 判断是否持有锁
           */
      public synchronized boolean isHoldLock(TransactionId tid, PageId pageId) {
          ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
          if (locks == null) {
              return false;
          }
          PageLock pageLock = locks.get(tid);
          if (pageLock == null) {
              return false;
          }
          return true;
      }
  
      /**
           * 完成事务后释放所有锁
           */
      public synchronized void completeTranslation(TransactionId tid) {
          // 遍历所有的页，如果对应事务持有锁就会释放
          for (PageId pageId : lockMap.keySet()) {
              releaseLock(tid, pageId);
          }
      }
  }
  ```

### 方法

- BufferPoll(int numPages)

  ```java
  public BufferPool(int numPages) {
      // some code goes here
      this.numPages = numPages;
      pageStore = new ConcurrentHashMap<>();
      head = new LinkedNode(new HeapPageId(-1, -1), null);
      tail = new LinkedNode(new HeapPageId(-1, -1), null);
      head.next = tail;
      tail.prev = head;
      // 新增一个锁管理器
      lockManager = new LockManager();
  }
  ```

- getPage(TransactionId tid, PageId pid, Permissions perm)

  ```java
  public Page getPage(TransactionId tid, PageId pid, Permissions perm)
      throws TransactionAbortedException, DbException {
      // some code goes here
      // ------------------- lab 5 -----------------------------
      // 判断需要获取的锁的类型
      int lockType = perm == Permissions.READ_ONLY ? PageLock.SHARE : PageLock.EXCLUSIVE;
      // 计算超时时间（设置为 500 ms）
      long startTime = System.currentTimeMillis();
      boolean isAcquired = false;
      // 循环获取锁
      while(!isAcquired){
          // 尝试获取锁
          isAcquired = lockManager.acquiredLock(pid, tid, lockType);
          long now = System.currentTimeMillis();
          // 如果超过 500 ms没有获取就抛出异常
          if(now - startTime > 500){
              // 放弃当前事务
              throw new TransactionAbortedException();
          }
      }
  
      // ------------------- lab 3 ------------------------------
      // 如果缓存池中没有
      if(!pageStore.containsKey(pid)){
          // 获取
          DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
          Page page = dbFile.readPage(pid);
          // 是否超过大小
          if(pageStore.size() >= numPages){
              // 使用 LRU 算法进行淘汰最近最久未使用
              evictPage();
          }
          LinkedNode node = new LinkedNode(pid, page);
          // 放入缓存
          pageStore.put(pid, node);
          // 插入头节点
          addToHead(node);
      }
      // 移动到头部
      moveToHead(pageStore.get(pid));
      // 从 缓存池 中获取
      return pageStore.get(pid).page;
  }
  ```

- unsafeReleasePage(TransactionId tid, PageId pid)

  ```java
  public  void unsafeReleasePage(TransactionId tid, PageId pid) {
      // some code goes here
      // not necessary for lab1|lab2
      lockManager.releaseLock(tid, pid);
  }
  ```

### 全代码

```java
package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    // 锁
    class PageLock{
        private static final int SHARE = 0;
        private static final int EXCLUSIVE = 1;
        private TransactionId tid;
        private int type;
        public PageLock(TransactionId tid, int type){
            this.tid = tid;
            this.type = type;
        }
        public TransactionId getTid(){
            return tid;
        }
        public int getType(){
            return type;
        }
        public void setType(int type){
            this.type = type;
        }
    }

    class LockManager {
        ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap = new ConcurrentHashMap<>();
        /**
         * 获取锁
         */
        public synchronized boolean acquiredLock(PageId pageId, TransactionId tid, int requiredType) {
            // 判断当前页是否当前有锁
            if (lockMap.get(pageId) == null) {
                // 创建锁
                PageLock pageLock = new PageLock(tid, requiredType);
                ConcurrentHashMap<TransactionId, PageLock> pageLocks = new ConcurrentHashMap<>();
                pageLocks.put(tid, pageLock);
                lockMap.put(pageId, pageLocks);
                return true;
            }
            // 获取当前锁的等待队列
            ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pageId);
            // tid 没有 Page 上的锁
            if (pageLocks.get(tid) == null) {
                // 如果 当前页已经有锁
                if (pageLocks.size() > 1) {
                    // 如果是共享锁，新增获取对象
                    if (requiredType == PageLock.SHARE) {
                        // tid 请求锁
                        PageLock pageLock = new PageLock(tid, PageLock.SHARE);
                        pageLocks.put(tid, pageLock);
                        lockMap.put(pageId, pageLocks);
                        return true;
                    }
                    // 如果是排他锁
                    else if (requiredType == PageLock.EXCLUSIVE) {
                        // tid 需要获取写锁，拒绝
                        return false;
                    }
                }
                if (pageLocks.size() == 1) {
                    // page 上有一个其他事务， 可能是写锁，也可能是读锁
                    PageLock curLock = null;
                    for (PageLock lock : pageLocks.values()) {
                        curLock = lock;
                    }
                    if (curLock.getType() == PageLock.SHARE) {
                        // 如果请求的锁也是读锁
                        if (requiredType == PageLock.SHARE) {
                            // tid 请求锁
                            PageLock pageLock = new PageLock(tid, PageLock.SHARE);
                            pageLocks.put(tid, pageLock);
                            lockMap.put(pageId, pageLocks);
                            return true;
                        }
                        // 如果是独占锁
                        else if (requiredType == PageLock.EXCLUSIVE) {
                            // tid 需要获取写锁，拒绝
                            return false;
                        }
                    }
                    // 如果是独占锁
                    else if (curLock.getType() == PageLock.EXCLUSIVE) {
                        // tid 需要获取写锁，拒绝
                        return false;
                    }
                }
            }
            // 当前事务持有 Page 锁
            else if (pageLocks.get(tid) != null) {
                PageLock pageLock = pageLocks.get(tid);
                // 事务上是写锁
                if (pageLock.getType() == PageLock.SHARE) {
                    // 新请求的是读锁
                    if (requiredType == PageLock.SHARE) {
                        return true;
                    }
                    // 新请求是写锁
                    else if (requiredType == PageLock.EXCLUSIVE) {
                        // 如果该页面上只有一个锁，就是该事务的读锁
                        if (pageLocks.size() == 1) {
                            // 进行锁升级(升级为写锁)
                            pageLock.setType(PageLock.EXCLUSIVE);
                            pageLocks.put(tid, pageLock);
                            return true;
                        }
                        // 大于一个锁，说明有其他事务有共享锁
                        else if (pageLocks.size() > 1) {
                            // 不能进行锁的升级
                            return false;
                        }
                    }
                }
                // 事务上是写锁
                // 无论请求的是读锁还是写锁，都可以直接返回获取
                return pageLock.getType() == PageLock.EXCLUSIVE;
            }
            return false;
        }

        /**
         * 释放锁
         */
        public synchronized boolean releaseLock(TransactionId tid, PageId pageId) {
            // 判断是否持有锁
            if (isHoldLock(tid, pageId)) {
                ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
                locks.remove(tid);
                if (locks.size() == 0) {
                    lockMap.remove(pageId);
                }
                return true;
            }
            return false;
        }

        /**
         * 判断是否持有锁
         */
        public synchronized boolean isHoldLock(TransactionId tid, PageId pageId) {
            ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
            if (locks == null) {
                return false;
            }
            PageLock pageLock = locks.get(tid);
            if (pageLock == null) {
                return false;
            }
            return true;
        }

        /**
         * 完成事务后释放所有锁
         */
        public synchronized void completeTranslation(TransactionId tid) {
            // 遍历所有的页，如果对应事务持有锁就会释放
            for (PageId pageId : lockMap.keySet()) {
                releaseLock(tid, pageId);
            }
        }
    }

    // 锁管理
    private LockManager lockManager;

    // 页面的最大数量
    private final int numPages;
    // 储存的页面
    // key 为 PageId
    private final ConcurrentHashMap<PageId, LinkedNode> pageStore;
    // 页面的访问顺序
    private static class LinkedNode{
        PageId pageId;
        Page page;
        LinkedNode prev;
        LinkedNode next;
        public LinkedNode(PageId pageId, Page page){
            this.pageId = pageId;
            this.page = page;
        }
    }
    // 头节点
    LinkedNode head;
    // 尾节点
    LinkedNode tail;
    private void addToHead(LinkedNode node){
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void remove(LinkedNode node){
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(LinkedNode node){
        remove(node);
        addToHead(node);
    }

    private LinkedNode removeTail(){
        LinkedNode node = tail.prev;
        remove(node);
        return node;
    }



    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageStore = new ConcurrentHashMap<>();
        head = new LinkedNode(new HeapPageId(-1, -1), null);
        tail = new LinkedNode(new HeapPageId(-1, -1), null);
        head.next = tail;
        tail.prev = head;
        lockManager = new LockManager();
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        // ------------------- lab 5 -----------------------------
        // 判断需要获取的锁的类型
        int lockType = perm == Permissions.READ_ONLY ? PageLock.SHARE : PageLock.EXCLUSIVE;
        // 计算超时时间（设置为 500 ms）
        long startTime = System.currentTimeMillis();
        boolean isAcquired = false;
        // 循环获取锁
        while(!isAcquired){
            // 尝试获取锁
            isAcquired = lockManager.acquiredLock(pid, tid, lockType);
            long now = System.currentTimeMillis();
            // 如果超过 500 ms没有获取就抛出异常
            if(now - startTime > 500){
                // 放弃当前事务
                throw new TransactionAbortedException();
            }
        }

        // ------------------- lab 3 ------------------------------
        // 如果缓存池中没有
        if(!pageStore.containsKey(pid)){
            // 获取
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            // 是否超过大小
            if(pageStore.size() >= numPages){
                // 使用 LRU 算法进行淘汰最近最久未使用
                evictPage();
            }
            LinkedNode node = new LinkedNode(pid, page);
            // 放入缓存
            pageStore.put(pid, node);
            // 插入头节点
            addToHead(node);
        }
        // 移动到头部
        moveToHead(pageStore.get(pid));
        // 从 缓存池 中获取
        return pageStore.get(pid).page;
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
        lockManager.releaseLock(tid, pid);
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
        // 获取 数据库文件 DBfile
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        // 将页面刷新到缓存中
        updateBufferPoll(heapFile.insertTuple(tid, t), tid);
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
        // 查询所属表对应的文件
        HeapFile heapFile = (HeapFile)Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        // 将页面刷新到缓存中
        updateBufferPoll(heapFile.deleteTuple(tid, t), tid);
    }

    /**
     * 更新缓存
     * @param pageList 需要更新的页面
     * @param tid 事务id
     * */
    private void updateBufferPoll(List<Page> pageList, TransactionId tid) throws DbException {
        for (Page page : pageList){
            page.markDirty(true, tid);
            // 如果缓存池已满，执行淘汰策略
            if(pageStore.size() > numPages){
                evictPage();
            }
            // 获取节点，此时的页一定已经在缓存了，因为刚刚被修改的时候就已经放入缓存了
            LinkedNode node = pageStore.get(page.getId());
            // 更新新的页内容
            node.page = page;
            // 更新到缓存
            pageStore.put(page.getId(), node);
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
        for(PageId pageId : pageStore.keySet()){
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // 删除使用记录
        remove(pageStore.get(pid));
        // 删除缓存
        pageStore.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageStore.get(pid).page;
        // 如果是是脏页
        if(page.isDirty() != null){
            // 写入脏页
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            // 移除脏页标签 和 事务标签
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // 淘汰尾部节点
        LinkedNode node = removeTail();
        // 更新页面
        try{
            flushPage(node.pageId);
        }catch (IOException e){
            e.printStackTrace();
        }
        // 移除缓存中的记录
        pageStore.remove(node.pageId);
    }

}

```



## HeapFile

### 方法

- public List<Page> insertTuple(TransactionId tid, Tuple t)

  ```java
  public List<Page> insertTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
      // some code goes here
      // not necessary for lab1
  
      ArrayList<Page> list = new ArrayList<>();
      // 查询现有的页
      for (int pageNo = 0; pageNo < numPages(); pageNo++) {
          // 查询页
          HeapPageId pageId = new HeapPageId(getId(), pageNo);
          HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
          // 看当前页是有 空闲空间
          if(page.getNumEmptySlots() != 0){
              page.insertTuple(t);
              list.add(page);
              return list;
          }
          //----------------- lab 4 ------------------------
          // 当该 page 上没有空闲空 slot 的时候，释放该 page 上的锁，避免影响其他事务访问
          else{
              Database.getBufferPool().unsafeReleasePage(tid, pageId);
          }
      }
  
      // 如果所有页都已经写满，就要新建新的页面来加入(记得开启 append = true 也就是增量增加)
      BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file, true));
      // 新建一个空的页
      byte[] emptyPage = HeapPage.createEmptyPageData();
      output.write(emptyPage);
      // close 前会调用 flush() 刷盘到文件
      output.close();
  
      // 创建新的页面
      HeapPageId pageId = new HeapPageId(getId(), numPages() - 1);
      HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
      page.insertTuple(t);
      list.add(page);
      return list;
  }
  ```

  

## 测试

LockingTest





# Exercise 3

## BufferPoll

原本的设置是，如果页面被替换出去就要刷新页面，现在要修改的是当该页是脏页，也就是事务还没有提交的页面的时候，不能够刷新。

### 方法

- void evictPage()

  ```java
  private synchronized  void evictPage() throws DbException {
      // some code goes here
      // not necessary for lab1
      // 遍历全部页面，如果是脏页需要跳过，因为还没提交
      for (int i = 0; i < numPages; i++) {
          // 淘汰尾部节点
          LinkedNode node = removeTail();
          Page evictPage = node.page;
          // 如果当前有事务持有，也就是脏页，需要跳过
          if(evictPage.isDirty() != null){
              addToHead(node);
          }
          // 如果不是，刷盘
          else{
              // 更新页面
              try{
                  flushPage(node.pageId);
              }catch (IOException e){
                  e.printStackTrace();
              }
              pageStore.remove(node.pageId);
              return ;
          }
      }
      // 如果没有就是所有页面都是未提交的脏页
      throw new DbException("All Page Are Dirty Page");
  }
  ```



## 测试

实验三暂时无需测试



# Exercise 4

实现第二个 TransactionComplete，也就是验证提交成功的时候，刷新页面，提交失败的时候，回滚页面

## 方法

- flushPages(TransactionId tid)；   //事务提交成功后刷新

  ```java
      public synchronized  void flushPages(TransactionId tid) throws IOException {
          // some code goes here
          // not necessary for lab1|lab2
          // 遍历缓存中的所有页面，看是否是当前事务修改的页面
          for(LinkedNode node : pageStore.values()){
              PageId pageId = node.pageId;
              Page page = node.page;
              // 如果脏页的 事务id 相同
              if(tid.equals(page.isDirty())){
                  flushPage(pageId);
              }
          }
      }
  ```

- transactionComplete(TransactionId tid, boolean commit)

  ```java
  public void transactionComplete(TransactionId tid, boolean commit) {
      // some code goes here
      // not necessary for lab1|lab2
      // 如果成功提交
      if(commit){
          // 刷新页面
          try{
              flushPages(tid);
          }catch (IOException e){
              e.printStackTrace();
          }
      }
      // 如果提交失败，回滚
      else{
          restorePages(tid);
      }
      // 事务完成
      lockManager.completeTranslation(tid);
  }
  ```

- restorePages(TransactionId tid);      // 回滚页面

  ```java
      public synchronized void restorePages(TransactionId tid){
          // 遍历缓存中的所有页面，看是否是当前事务修改的页面
          for(LinkedNode node : pageStore.values()){
              PageId pageId = node.pageId;
              Page page = node.page;
              // 如果脏页的 事务id 相同
              if(tid.equals(page.isDirty())){
                  int tableId = pageId.getTableId();
                  // 获取现有的表
                  DbFile table = Database.getCatalog().getDatabaseFile(tableId);
                  // 读取当前的页面
                  Page pageFromDisk = table.readPage(pageId);
  
                  // 写回内存
                  node.page = pageFromDisk;
                  pageStore.put(pageId, node);
                  moveToHead(node);
              }
          }
      }
  ```

  

## 测试

TransactionTest、AbortEvictionTest（systemtest）



# Exercise 5

## 测试

TransactionTest