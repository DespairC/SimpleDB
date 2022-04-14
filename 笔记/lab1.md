## 资料来源

课程地址：[6.830/6.814: Database Systems (mit.edu)](http://db.lcs.mit.edu/6.830/index.php)

源码 + 讲义：[github.com](https://github.com/MIT-DB-Class/simple-db-hw-2021)

克隆到本地

```git
git clone https://github.com/MIT-DB-Class/simple-db-hw-2021.git 
```

## lab1

### Tuple 元组

#### **参数**：

- TupleDesc  表头部
  - 关系是相当于列头部
- RecordId 路径
- Field[] 存储的数据

#### **方法**：

- Tuple(TupleDesc)
  - 以表头部创建

#### 全代码：

```java
package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;
    // 路径
    private RecordId rid;
    private final Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length - 1; i++) {
            sb.append(fields[i]).append(" ");
        }
        sb.append(fields[fields.length - 1]);
        return sb.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return Arrays.asList(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tupleDesc = td;
    }
}
```

### TupleDesc 元组头部

#### **参数：**

- TDItem[] tdItems 每个字段对应的类型和名称
  - 内部类 TDItem
    - Type typeAr 字段类型
    - String fieldAr 字段名称


#### **方法**：

- TupleDesc(Type[] typeAr, String[] fieldAr)

- TupleDesc(Type[] typeAr)    默认名字为空 ("")

- iterator()

  - 直接转换

    ```java
    public Iterator<TDItem> iterator() {
    	return Arrays.asList(tdItems).iterator();
    }
    ```

- getSize（） 获取所有字段的类型大小

- TupleDesc merge()  ：合并，遍历两个表合成一个新TupleDesc 返回

- equals

  - 三个步骤:  比较 类 - 》 比较大小是否相同 -》比较每个字段

    ```java
    public boolean equals(Object o) {
        // some code goes here
        // 比较 class
        if(this.getClass().isInstance(o)){
            // 强转类型
            TupleDesc tupleDesc = (TupleDesc) o;
            // 比较大小是否相同
            if(numFields() == tupleDesc.numFields()){
                // 遍历每个字段
                for (int i = 0; i < numFields(); i++) {
                    // 如果类型不相等
                    if(!tdItems[i].fieldType.equals(tupleDesc.tdItems[i].fieldType)){
                        return false;
                    }
                }
                // 全部相等
                return true;
            }
        }
        return false;
    }
    ```

  - 关于比较类

    - class.isInstance(obj) 是判断类是否能够转化为当前类（编译器运行时才进行类型检查）
    - obj instanceof ( class ) 是判断类是否相同（编译时编译器需要知道类的具体类型）

#### **全代码：**

```java
package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private final TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.asList(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], "");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < tdItems.length; i++) {
            if(tdItems[i].fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException("not found fieldName " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < tdItems.length; i++) {
            size += tdItems[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] name = new String[td1.numFields() + td2.numFields()];
        for (int i = 0; i < td1.numFields(); i++) {
            types[i] = td1.tdItems[i].fieldType;
            name[i] = td1.tdItems[i].fieldName;
        }
        int len1 = td1.numFields();
        for (int i = 0; i < td2.numFields(); i++) {
            types[i + len1] = td2.tdItems[i].fieldType;
            name[i + len1] = td2.tdItems[i].fieldName;
        }
        return new TupleDesc(types, name);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // 比较 class
        if(this.getClass().isInstance(o)){
            // 强转类型
            TupleDesc tupleDesc = (TupleDesc) o;
            // 比较大小是否相同
            if(numFields() == tupleDesc.numFields()){
                // 遍历每个字段
                for (int i = 0; i < numFields(); i++) {
                    // 如果类型不相等
                    if(!tdItems[i].fieldType.equals(tupleDesc.tdItems[i].fieldType)){
                        return false;
                    }
                }
                // 全部相等
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tdItems.length - 1; i++) {
            sb.append(tdItems[i].fieldType).append("(").append(tdItems[i].fieldName).append("),");
        }
        // 处理最后一个
        sb.append(tdItems[tdItems.length - 1].fieldType).append("(").append(tdItems[tdItems.length - 1].fieldName).append("),");
        return sb.toString();
    }
}
```

### Field 字段

- IntField
  - int value
- StringField
  - String value
  - int maxSize 最大大小，超过最大大小分割

### 测试：

TupleTest 和 TupleDescTest



## lab2

### Catelog 表日志

存储所有表记录

#### **参数：**

- ConcurrentHashMap<Integer, Table> hashTable 用于存储 tableId 和 表记录的映射

#### **方法：**

- Catalog()：创建hashTable

- addTable(DbFile file, String name, String pkeyField)： put新值进去

- getTableId(String name)：根据名字查询tableId

  ```java
  public int getTableId(String name) throws NoSuchElementException {
      // some code goes here
      // 遍历
      Integer res = hashTable.searchValues(1, value ->{
          if(value.tableName.equals(name)){
              return value.file.getId();
          }
          return null;
      });
      if(res != null){
          return res;
      }
      throw new NoSuchElementException("not found id for table " + name);
  }
  ```

- TupleDesc getTupleDesc(int tableid)：根据表id获得元组头部

  ```java
  public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
      // some code goes here
      Table t = hashTable.getOrDefault(tableid, null);
      if(t != null){
          return t.file.getTupleDesc();
      }
      throw new NoSuchElementException("not found tupleDesc for table " + tableid);
  }
  ```

- tableIdIterator

  ```java
  public Iterator<Integer> tableIdIterator() {
      // some code goes here
      return hashTable.keySet().iterator();
  }
  ```

#### **全代码**：

```java
package simpledb.common;

import simpledb.common.Type;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    // 所有表
    private final ConcurrentHashMap<Integer, Table> hashTable;

    /**
     * 表
     * */
    private static class Table{
        private DbFile file;
        private String tableName;
        private String pkeyField;

        public Table(DbFile file, String tableName, String pkeyField){
            this.file = file;
            this.tableName = tableName;
            this.pkeyField = pkeyField;
        }

        public String toString(){
            return tableName + "(" + file.getId() + ")" + pkeyField + ")";
        }
    }


    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        hashTable = new ConcurrentHashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        hashTable.put(file.getId(), new Table(file, name, pkeyField));
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        // 遍历
        Integer res = hashTable.searchValues(1, value ->{
            if(value.tableName.equals(name)){
                return value.file.getId();
            }
            return null;
        });
        if(res != null){
            return res;
        }
        throw new NoSuchElementException("not found id for table " + name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        Table t = hashTable.getOrDefault(tableid, null);
        if(t != null){
            return t.file.getTupleDesc();
        }
        throw new NoSuchElementException("not found tupleDesc for table " + tableid);
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable`
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Table t = hashTable.getOrDefault(tableid, null);
        if(t != null){
            return t.file;
        }
        throw new NoSuchElementException("not found dbFile for table " + tableid);
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        Table t = hashTable.getOrDefault(tableid, null);
        if(t != null){
            return t.pkeyField;
        }
        throw new NoSuchElementException("not found primaryKey for table " + tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return hashTable.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        Table t = hashTable.getOrDefault(id, null);
        if(t != null){
            return t.tableName;
        }
        throw new NoSuchElementException("not found tableName for table " + id);
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        hashTable.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        // 根目录
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            // 读取 catelogFile
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
```

### 测试：

CatalogTest

## lab3

### BufferPoll 缓冲池

#### 参数

- private static final int DEFAULT_PAGE_SIZE = 4096;
- private static int pageSize = DEFAULT_PAGE_SIZE;     //每页最大字节数
- public static final int DEFAULT_PAGES = 50  默认页面数
- private final int numPages  页面的最大数量
- private final ConcurrentHashMap<Integer, Page> pageStore   储存的页面

#### 方法

- getPage(TransactionId tid, PageId pid, Permissions perm)

  ```java
  public Page getPage(TransactionId tid, PageId pid, Permissions perm)
      throws TransactionAbortedException, DbException {
      // some code goes here
      // 如果缓存池中没有
      if(!pageStore.containsKey(pid.hashCode())){
          // 获取
          DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
          Page page = dbFile.readPage(pid);
          // 是否超过大小
          if(pageStore.size() >= numPages){
              // 淘汰 (后面的 lab 书写)
              throw new DbException("页面已满");
          }
          // 放入缓存
          pageStore.put(pid.hashCode(), page);
      }
      // 从 缓存池 中获取
      return pageStore.get(pid.hashCode());
  }
  ```

#### 全代码

```java
package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

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

    // 页面的最大数量
    private final int numPages;
    // 储存的页面
    private final ConcurrentHashMap<Integer, Page> pageStore;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageStore = new ConcurrentHashMap<>();
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
        // 如果缓存池中没有
        if(!pageStore.containsKey(pid.hashCode())){
            // 获取
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            // 是否超过大小
            if(pageStore.size() >= numPages){
                // 淘汰 (后面的 lab 书写)
                throw new DbException("页面已满");
            }
            // 放入缓存
            pageStore.put(pid.hashCode(), page);
        }
        // 从 缓存池 中获取
        return pageStore.get(pid.hashCode());
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
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

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
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
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
    }
}
```

### 测试：

本节暂时不用测试，在下一节 HeapPage中 readPage会有测试



## lab4

### HeapPage  页面

#### 参数

- final HeapPageId pid;
- final TupleDesc td; // 表头部 也是元组头部
  - 这个的意义是用来统计每行所占的大小，一个表对应一个页面
- final byte[] header;// 头部数据 bitmap
- final Tuple[] tuples; // 元组数据
- final int numSlots; // 槽数，也就是行的数量
- byte[] oldData;
- private final Byte oldDataLock= (byte) 0;

#### 方法

- getNumTuples()  返回一个页面有多少个元组

  公式：tuple_nums = floor((page_size * 8) / tuple_size * 8 + 1)

  - 向下取整，剩下不够一个元组也就没办法储存了，需要抛弃
  - 1字节 =  8 比特
  - tuple_size * 8 + 1 ：除了数据需要的大小，还需要一个bit位标记是否已被使用

  ```java
  private int getNumTuples() {        
      // some code goes here
      // 计算页面有多少个元组
      return (int) Math.floor((BufferPool.getPageSize() * 8 * 1.0) / (td.getSize() * 8 + 1));
  }
  ```

- HeapPage(HeapPageId id, byte[] data) 创建一个页面，也就是读取数据

  ```java
      public HeapPage(HeapPageId id, byte[] data) throws IOException {
          this.pid = id;
          this.td = Database.getCatalog().getTupleDesc(id.getTableId());
          this.numSlots = getNumTuples();
          DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
  
          // allocate and read the header slots of this page
          // 处理头部
          header = new byte[getHeaderSize()];
          for (int i=0; i<header.length; i++)
              header[i] = dis.readByte();
          // 处理每行
          tuples = new Tuple[numSlots];
          try{
              // allocate and read the actual records of this page
              for (int i=0; i<tuples.length; i++)
                  tuples[i] = readNextTuple(dis,i);
          }catch(NoSuchElementException e){
              e.printStackTrace();
          }
          dis.close();
  
          setBeforeImage();
      }
  ```

- getHeaderSize()：获取头部长度

  - headerBytes = ceiling(tuplePerPage / 8);
    - 向上取整，满足无用行，凑8的整数倍

  ```java
  private int getHeaderSize() {
      // some code goes here
      // headerBytes = ceiling(tuplePerPage / 8);
      return (int) Math.ceil(getNumTuples() * 1.0 / 8);
  }
  ```

- isSlotUsed(int i)：该槽位是否被使用

  ```java
  public boolean isSlotUsed(int i) {
      // some code goes here
      // 槽位
      int quot = i / 8;
      // 偏移
      int move = i % 8;
      // 获得对应的槽位
      int bitidx = header[quot];
      // 偏移 move 位，看是否等于 1
      return ((bitidx >> move) & 1) == 1;
  }
  ```

- iterator()

  ```java
  public Iterator<Tuple> iterator() {
      // some code goes here
      // 获取已使用的槽对应的数
      ArrayList<Tuple> res = new ArrayList<>();
      for (int i = 0; i < numSlots; i++) {
          if(isSlotUsed(i)){
              res.add(tuples[i]);
          }
      }
      return res.iterator();
  }
  ```

#### 全代码

```java
package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    // 槽储存
    final byte[] header;
    // 元组数据
    final Tuple[] tuples;
    // 槽数
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        // 处理头部
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        // 处理每行
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        // 计算页面有多少个元组
        // tuple_nums = floor((page_size * 8) / tuple_size * 8 + 1)
        return (int) Math.floor((BufferPool.getPageSize() * 8 * 1.0) / (td.getSize() * 8 + 1));

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        // headerBytes = ceiling(tuplePerPage / 8);
        return (int) Math.ceil(getNumTuples() * 1.0 / 8);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return null;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int count = 0;
        for (int i = 0; i < numSlots; i++) {
            if(!isSlotUsed(i)){
                count++;
            }
        }
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        // 槽位
        int quot = i / 8;
        // 偏移
        int move = i % 8;
        // 获得对应的槽位
        int bitidx = header[quot];
        // 偏移 move 位，看是否等于 1
        return ((bitidx >> move) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        // 获取已使用的槽对应的数
        ArrayList<Tuple> res = new ArrayList<>();
        for (int i = 0; i < numSlots; i++) {
            if(isSlotUsed(i)){
                res.add(tuples[i]);
            }
        }
        return res.iterator();
    }

}
```

### RecordId 路径

#### 参数

- private PageId pid;		//页面 id
- private int tupleno;       // 行id

#### 全代码

```java
package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pid;
    private int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if(o instanceof RecordId){
            RecordId recordId = (RecordId) o;
            if (pid.equals(recordId.pid) && tupleno == recordId.tupleno){
                return true;
            }
        }
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        String hash = "" + pid.getTableId() + pid.getPageNumber() + tupleno;
        return hash.hashCode();
    }

}

```

### HeapPageId 页面id

#### 参数

- private final int tableId;		//对应的表id
- private final int pgNo;          // 页面id

#### 全代码

```java
package simpledb.storage;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {
    private final int tableId;
    private final int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        String hash = "" + tableId + pgNo;
        return hash.hashCode();
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if(o instanceof PageId){
            PageId page = (PageId) o;
            if(page.getTableId() == tableId && page.getPageNumber() == pgNo){
                return true;
            }
        }
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }
}
```

### 测试：

HeapPageIdTest 、HeapPageReadTest、RecordIdTest



## lab5

### HeapFile 存储文件

#### 参数

- private final File file;	//文件
- private final TupleDesc tupleDesc;    // 表头

#### 方法

- getId()：由绝对路径生成id

  ```java
  public int getId() {
      // some code goes here
      // 文件的绝对路径，取hash。独一无二的id
      return file.getAbsoluteFile().hashCode();
  }
  ```

- numsPage()：计算文件有多少页

  ```java
  public int numPages() {
      // some code goes here
      // 文件长度 / 每页的字节数
      int res = (int) Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
      return res;
  }
  ```

- readPage()：读取页面

  ```java
      public Page readPage(PageId pid) {
          // some code goes here
          // 表id
          int tableId = pid.getTableId();
          // 该表所处的页码
          int pgNo = pid.getPageNumber();
          // 随机访问,指针偏移访问
          RandomAccessFile f = null;
          try{
              // 读取当前文件
              f = new RandomAccessFile(file, "r");
              // 当前页号 * 每页的字节大小 是否超出文件的范围
              if((pgNo + 1) * BufferPool.getPageSize() > f.length()){
                  f.close();
                  throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
              }
              // 用于储存
              byte[] bytes = new byte[BufferPool.getPageSize()];
              // 指针偏移
              f.seek(pgNo * BufferPool.getPageSize());
              // 读取(返回读取的数量)
              int read = f.read(bytes, 0, BufferPool.getPageSize());
              // 如果取出来少了，说明不存在
              if(read != BufferPool.getPageSize()){
                  throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
              }
              return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), bytes);
          }catch (IOException e){
              e.printStackTrace();
          }finally {
              try{
                  // 关闭流
                  f.close();
              }catch (IOException e){
                  e.printStackTrace();
              }
          }
          throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
      }
  ```

- DbFileIterator iterator(TransactionId tid)：迭代器，需要写一个内部类

  实现DbFileIterator 

  ```java
  public DbFileIterator iterator(TransactionId tid) {
          // some code goes here
          return new HeapFileIterator(this, tid);
  }
  
  private static final class HeapFileIterator implements DbFileIterator{
      private final HeapFile heapFile;
      private final TransactionId tid;
      // 元组迭代器
      private Iterator<Tuple> iterator;
      private int whichPage;
  
      public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
          this.heapFile = heapFile;
          this.tid = tid;
      }
  
      @Override
      public void open() throws DbException, TransactionAbortedException {
          // 获取第一页的全部元组
          whichPage = 0;
          iterator = getPageTuple(whichPage);
      }
  
      // 获取当前页的所有行
      private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException {
          // 在文件范围内
          if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
              HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
              // 从缓存池中查询相应的页面 读权限
              HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
              return page.iterator();
          }
          throw new DbException(String.format("heapFile %d not contain page %d", pageNumber, heapFile.getId()));
      }
  
      @Override
      public boolean hasNext() throws DbException, TransactionAbortedException {
          // 如果迭代器为空
          if(iterator == null){
              return false;
          }
          // 如果已经遍历结束
          if(!iterator.hasNext()){
              // 是否还存在下一页，小于文件的最大页
              if(whichPage < (heapFile.numPages() - 1)){
                  whichPage++;
                  // 获取下一页
                  iterator = getPageTuple(whichPage);
                  return iterator.hasNext();
              }
              // 所有元组获取完毕
              else{
                  return false;
              }
          }
          return true;
      }
  
      @Override
      public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
          // 如果没有元组了，抛出异常
          if(iterator == null || !iterator.hasNext()){
              throw new NoSuchElementException();
          }
          // 返回下一个元组
          return iterator.next();
      }
  
      @Override
      public void rewind() throws DbException, TransactionAbortedException {
          // 清除上一个迭代器
          close();
          // 重新开始
          open();
      }
  
      @Override
      public void close() {
          iterator = null;
      }
  }
  ```

#### 全代码

```java
package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // 文件的绝对路径，取hash。独一无二的id
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // 表id
        int tableId = pid.getTableId();
        // 该表所处的页码
        int pgNo = pid.getPageNumber();
        // 随机访问,指针偏移访问
        RandomAccessFile f = null;
        try{
            // 读取当前文件
            f = new RandomAccessFile(file, "r");
            // 当前页号 * 每页的字节大小 是否超出文件的范围
            if((pgNo + 1) * BufferPool.getPageSize() > f.length()){
                f.close();
                throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
            }
            // 用于储存
            byte[] bytes = new byte[BufferPool.getPageSize()];
            // 指针偏移
            f.seek(pgNo * BufferPool.getPageSize());
            // 读取(返回读取的数量)
            int read = f.read(bytes, 0, BufferPool.getPageSize());
            // 如果取出来少了，说明不存在
            if(read != BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
            }
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), bytes);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                // 关闭流
                f.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("表 %d 页 %d 不存在", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // 文件长度 / 每页的字节数
        int res = (int) Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
        return res;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{
        private final HeapFile heapFile;
        private final TransactionId tid;
        // 元组迭代器
        private Iterator<Tuple> iterator;
        private int whichPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // 获取第一页的全部元组
            whichPage = 0;
            iterator = getPageTuple(whichPage);
        }

        // 获取当前页的所有行
        private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException {
            // 在文件范围内
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                // 从缓存池中查询相应的页面 读权限
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException(String.format("heapFile %d not contain page %d", pageNumber, heapFile.getId()));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // 如果迭代器为空
            if(iterator == null){
                return false;
            }
            // 如果已经遍历结束
            if(!iterator.hasNext()){
                // 是否还存在下一页，小于文件的最大页
                if(whichPage < (heapFile.numPages() - 1)){
                    whichPage++;
                    // 获取下一页
                    iterator = getPageTuple(whichPage);
                    return iterator.hasNext();
                }
                // 所有元组获取完毕
                else{
                    return false;
                }
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // 如果没有元组了，抛出异常
            if(iterator == null || !iterator.hasNext()){
                throw new NoSuchElementException();
            }
            // 返回下一个元组
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // 清除上一个迭代器
            close();
            // 重新开始
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}

```

### 测试：

HeapFileReadTest

## lab6

### SeqScan 查询

#### 参数

- private final TransactionId tid	// 事务id
- private int tableid    // 要查询的表id
- private String tableAlias    // 表别名
- private DbFileIterator iterator   //迭代器，用于检索

#### 方法

- getTableName()：获取table的名称

  通过Catelog查询

  ```java
  public String getTableName() {
      return Database.getCatalog().getTableName(tableid);
  }
  ```

- open()：开启迭代器，也就是获取迭代器

  - 这里的迭代器其实就是使用之前写的 `HeapFileIterator`，所以主要代码还是在之前的内部类中，这里只是调用

  ```java
  public void open() throws DbException, TransactionAbortedException {
      // some code goes here
      // 查询目录 -》 根据表id查询相应的 DBFile -> 获取迭代器
      iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
      iterator.open();
  }
  ```

- getTupleDesc()：通过Catelog获取TupleDesc，并修改为别名

  ```java
  public TupleDesc getTupleDesc() {
      // some code goes here
      TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableid);
      String prefix = tableAlias != null? tableAlias : "null";
      // 遍历，添加前缀
      int len = tupleDesc.numFields();
      Type[] types = new Type[len];
      String[] fieldNames = new String[len];
      for (int i = 0; i < len; i++) {
          types[i] = tupleDesc.getFieldType(i);
          fieldNames[i] = prefix + "." + tupleDesc.getFieldName(i);
      }
      return new TupleDesc(types, fieldNames);
  }
  ```

#### 全代码

```java
package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        // 查询目录 -》 根据表id查询相应的 DBFile -> 获取迭代器
        iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        String prefix = tableAlias != null? tableAlias : "null";
        // 遍历，添加前缀
        int len = tupleDesc.numFields();
        Type[] types = new Type[len];
        String[] fieldNames = new String[len];
        for (int i = 0; i < len; i++) {
            types[i] = tupleDesc.getFieldType(i);
            fieldNames[i] = prefix + "." + tupleDesc.getFieldName(i);
        }
        return new TupleDesc(types, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(iterator == null){
            return false;
        }
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(iterator == null){
            throw new NoSuchElementException("No Next Tuple");
        }
        Tuple tuple = iterator.next();
        if(tuple == null){
            throw new NoSuchElementException("No Next Tuple");
        }
        return tuple;
    }

    public void close() {
        // some code goes here
        iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        iterator.rewind();
    }
}
```

### 测试：

## lab7

### 简易查询

#### 准备阶段

在 src/java/simpledb 下，创建一个test测试

创建一个db文件 test.txt

数据自定，比如

```
1,1,1,1
2,2,2,2
3,4,4,5

```

再将test.txt 转化为 二进制格式

需要调用HeapFileEncoder.convert()

#### HeapFileEncoder

```java
package simpledb.storage;

import simpledb.common.Type;
import simpledb.common.Utility;

import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 * HeapFileEncoder reads a comma delimited text file or accepts
 * an array of tuples and converts it to
 * pages of binary data in the appropriate format for simpledb heap pages
 * Pages are padded out to a specified length, and written consecutive in a
 * data file.
 */

public class HeapFileEncoder {

  /** Convert the specified tuple list (with only integer fields) into a binary
   * page file. <br>
   *
   * The format of the output file will be as specified in HeapPage and
   * HeapFile.
   *
   * @see HeapPage
   * @see HeapFile
   * @param tuples the tuples - a list of tuples, each represented by a list of integers that are
   *        the field values for that tuple.
   * @param outFile The output file to write data to
   * @param npagebytes The number of bytes per page in the output file
   * @param numFields the number of fields in each input tuple
   * @throws IOException if the temporary/output file can't be opened
   */
  public static void convert(List<List<Integer>> tuples, File outFile, int npagebytes, int numFields) throws IOException {
      // 创建临时文件
      File tempInput = File.createTempFile("tempTable", ".txt");
      tempInput.deleteOnExit();
      // 写流
      BufferedWriter bw = new BufferedWriter(new FileWriter(tempInput));
      // 遍历元组, 统计元组数量
      for (List<Integer> tuple : tuples) {
          int writtenFields = 0;
          for (Integer field : tuple) {
              writtenFields++;
              // 如果超过每个字段的元组数，抛出异常
              if (writtenFields > numFields) {
                  throw new RuntimeException("Tuple has more than " + numFields + " fields: (" +
                          Utility.listToString(tuple) + ")");
              }
              // 写入当前元组
              bw.write(String.valueOf(field));
              // 分隔
              if (writtenFields < numFields) {
                  bw.write(',');
              }
          }
          // 换行
          bw.write('\n');
      }
      bw.close();
      // 传入下一个当作输入文件
      convert(tempInput, outFile, npagebytes, numFields);
  }

      public static void convert(File inFile, File outFile, int npagebytes,
                 int numFields) throws IOException {
      // 创建类型，每一个赋初值位 INT
      Type[] ts = new Type[numFields];
          Arrays.fill(ts, Type.INT_TYPE);
      convert(inFile,outFile,npagebytes,numFields,ts);
      }

  public static void convert(File inFile, File outFile, int npagebytes,
                 int numFields, Type[] typeAr)
      throws IOException {
      convert(inFile,outFile,npagebytes,numFields,typeAr,',');
  }

   /** Convert the specified input text file into a binary
    * page file. <br>
    * Assume format of the input file is (note that only integer fields are
    * supported):<br>
    * int,...,int\n<br>
    * int,...,int\n<br>
    * ...<br>
    * where each row represents a tuple.<br>
    * <p>
    * The format of the output file will be as specified in HeapPage and
    * HeapFile.
    *
    * @see HeapPage
    * @see HeapFile
    * @param inFile The input file to read data from
    * @param outFile The output file to write data to
    * @param npagebytes The number of bytes per page in the output file
    * @param numFields the number of fields in each input line/output tuple
    * @throws IOException if the input/output file can't be opened or a
    *   malformed input line is encountered
    */
  public static void convert(File inFile, File outFile, int npagebytes,
                 int numFields, Type[] typeAr, char fieldSeparator)
      throws IOException {
      // 统计类型的总字节数
      int nrecbytes = 0;
      for (int i = 0; i < numFields ; i++) {
          nrecbytes += typeAr[i].getLen();
      }
      // 计算每页存储的的元组数量
      int nrecords = (npagebytes * 8) /  (nrecbytes * 8 + 1);  //floor comes for free
      
    //  per record, we need one bit; there are nrecords per page, so we need
    // nrecords bits, i.e., ((nrecords/32)+1) integers.
      // 计算所需要的物理大小，一字节存储 8 位，以字节为单位，向上取整
    int nheaderbytes = (nrecords / 8);
    if (nheaderbytes * 8 < nrecords)
        nheaderbytes++;  //ceiling
      //恢复成比特数量
    int nheaderbits = nheaderbytes * 8;

    BufferedReader br = new BufferedReader(new FileReader(inFile));
    FileOutputStream os = new FileOutputStream(outFile);

    // our numbers probably won't be much larger than 1024 digits
    char[] buf = new char[1024];

    // 字符指针
    int curpos = 0;
    // 行计数（元组计数）
    int recordcount = 0;
    int npages = 0;
    // 字符计数
    int fieldNo = 0;

    ByteArrayOutputStream headerBAOS = new ByteArrayOutputStream(nheaderbytes);
    DataOutputStream headerStream = new DataOutputStream(headerBAOS);
    ByteArrayOutputStream pageBAOS = new ByteArrayOutputStream(npagebytes);
    DataOutputStream pageStream = new DataOutputStream(pageBAOS);

    boolean done = false;
    boolean first = true;
    while (!done) {
        // 读取指针所指字符
        int c = br.read();
        
        // Ignore Windows/Notepad special line endings
        if (c == '\r')
            continue;

        // 换行跳到下一行
        if (c == '\n') {
            if (first)
                continue;
            // 行数 + 1
            recordcount++;
            first = true;
        } else
            first = false;


        // 如果等于 分隔符， 换行
        if (c == fieldSeparator || c == '\n' || c == '\r') {
            // 读取当前已经写入 buf 的字符串
            String s = new String(buf, 0, curpos);
            // 判断类型
            if (typeAr[fieldNo] == Type.INT_TYPE) {
                try {
                    // 删除前导空格和后置空格 然后转整型后写入
                    pageStream.writeInt(Integer.parseInt(s.trim()));
                } catch (NumberFormatException e) {
                    System.out.println ("BAD LINE : " + s);
                }
            }
            else   if (typeAr[fieldNo] == Type.STRING_TYPE) {
                s = s.trim();
                // 计算长度, 是否大于最大限制
                int overflow = Type.STRING_LEN - s.length();
                // 大于String允许最大限制，截取
                if (overflow < 0) {
                    s  = s.substring(0,Type.STRING_LEN);
                }
                // 写入字符串长度
                pageStream.writeInt(s.length());
                // 写入字符串（有可能是截取的）
                pageStream.writeBytes(s);
                // 如果未满，填充 byte 0
                while (overflow-- > 0)
                    pageStream.write((byte)0);
            }
            // 计数重置
            curpos = 0;
            // 换行
            if (c == '\n')
                // 字符计数重置
                fieldNo = 0;
            else
                // 字符计数 + 1
                fieldNo++;

            // 结束字符
        } else if (c == -1) {
            done = true;
            
        }
        // c 为字符，记录到buff
        else {
            buf[curpos++] = (char)c;
            continue;
        }
        
        // if we wrote a full page of records, or if we're done altogether,
        // write out the header of the page.
        //
        // in the header, write a 1 for bits that correspond to records we've
        // written and 0 for empty slots.
        //
        // when we're done, also flush the page to disk, but only if it has
        // records on it.  however, if this file is empty, do flush an empty
        // page to disk.

        // 元组总数是否大于等于每页的最大元组数， 是否结束 并且 元组总数 > 0， 结束 并且 页面 = 0
        // 也就是大于等于一页，需要提前分割
        if (recordcount >= nrecords
            || done && recordcount > 0
            || done && npages == 0) {
            int i = 0;
            byte headerbyte = 0;
            // 落到相应槽位，对应点置 1
            for (i=0; i<nheaderbits; i++) {
                // 每一行入槽位
                if (i < recordcount)
                    headerbyte |= (1 << (i % 8));

                // 写入当前槽位
                if (((i+1) % 8) == 0) {
                    headerStream.writeByte(headerbyte);
                    // 重置，下一槽位
                    headerbyte = 0;
                }
            }
            // 如果还有未满槽位，写入
            if (i % 8 > 0)
                headerStream.writeByte(headerbyte);
            
            // pad the rest of the page with zeroes
            // 填充满整页
            for (i=0; i<(npagebytes - (recordcount * nrecbytes + nheaderbytes)); i++)
                pageStream.writeByte(0);
            
            // write header and body to file
            headerStream.flush();
            headerBAOS.writeTo(os);
            pageStream.flush();
            pageBAOS.writeTo(os);
            
            // reset header and body for next page
            headerBAOS = new ByteArrayOutputStream(nheaderbytes);
            headerStream = new DataOutputStream(headerBAOS);
            pageBAOS = new ByteArrayOutputStream(npagebytes);
            pageStream = new DataOutputStream(pageBAOS);

            // 重置
            recordcount = 0;
            // 页面 + 1
            npages++;
        }
    }
    br.close();
    os.close();
  }
}
```

#### 代码：

```java
package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;

public class test {
    public static void main(String[] args) throws IOException {
        // 创建模式头部
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] name = new String[]{"field0", "field1", "field2", "field3"};
        TupleDesc tupleDesc = new TupleDesc(types, name);
        HeapFileEncoder.convert(new File("test.dat"), new File("some_date_file.dat"), BufferPool.getPageSize(), 4, types);
        File file = new File("some_date_file.dat");
        // 创建 table 文件
        HeapFile heapFile = new HeapFile(new File("some_date_file.dat"), tupleDesc);

        // 将table 文件 写入日志，表名test
        Database.getCatalog().addTable(heapFile, "test");

        // 创建事务 id
        TransactionId transactionId = new TransactionId();
        // 根据表 id 查询
        SeqScan scan = new SeqScan(transactionId, heapFile.getId());

        try{
            scan.open();
            while (scan.hasNext()){
                Tuple tuple = scan.next();
                System.out.println(tuple);
            }
            scan.close();
            Database.getBufferPool().transactionComplete(transactionId);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}

```

### 过程中遇到问题

在准备阶段的时候，如果最后一个数据没有换行，会导致最后一行丢失，并且多出部分数据

例如：

```
1,1,1,1
2,2,2,2
3,4,4,5
```

输出的结果是：

```
1,1,1,1
2,2,2,2
```



#### 最后一行丢失的问题

**原因**：

在 HeapFileEncoder 的代码中，有一个记录行数的字段叫 `recordcount` ，会记录当前的行总数

```java
// 换行跳到下一行
if (c == '\n') {
    if (first)
        continue;
    // 行数 + 1
    recordcount++;		// 这个地方是更新的时机
    first = true;
} else
    first = false;
```

如果没有换行的情况下，就不会有更新的时机。因此，在最后执行行写入槽位时会丢失一行

```java
// 落到相应槽位，对应点置 1
for (i=0; i<nheaderbits; i++) {
    // 每一行入槽位
    if (i < recordcount)   ---->>> //这个位置，由于少了一行
        headerbyte |= (1 << (i % 8));
    // 写入当前槽位
    if (((i+1) % 8) == 0) {
        headerStream.writeByte(headerbyte);
        // 重置，下一槽位
        headerbyte = 0;
    }
}
```

#### 大小的问题

在判断的时候，可以看到写入的时机是遇到分隔符和换行符

```java
// 如果等于 分隔符， 换行
if (c == fieldSeparator || c == '\n' || c == '\r') {
    // 读取当前已经写入 buf 的字符串
    String s = new String(buf, 0, curpos);
    // 判断类型
```

也就是说基于这种情况，如果最后没有去加上换行，就会丢失最后的一个字符。

可以尝试输出当前文件的大小来对比加换行符和不加换行符的区别

```java
输出：
	加换行符：4096
    不加换行符：4108
```

**原因**：

上面测试的数据是3行4列，每一个数字占4个字节。当最后一个字节丢失的时候，也就是多写入了三个字节，并且系统不知道这个情况，因为没有更新 recordcount ，在最后的写入中无法感知。

```java
// pad the rest of the page with zeroes
// 填充满整页
for (i=0; i<(npagebytes - (recordcount * nrecbytes + nheaderbytes)); i++)
    pageStream.writeByte(0);
```

npagebytes - (recordcount * nrecbytes + nheaderbytes) 就是

一页总的数量 - 行数 * 每行的比特数 + 头部所占的数

```
丢失了 1 行，也就是系统记录是 2 行。并且由于最后一个字符没有写入，所以是会多写入了 3 个数字，也就是 3 * 4 = 12 字节
也就是说 填充完0之后，理应是 4096，但由于多了 12 就变成 4096 + 12 = 4108 字节
```

