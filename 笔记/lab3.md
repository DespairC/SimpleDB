[toc]

# Exercise 1

## IntHistogram

这个主要是统计直方图对于某一个限制条件占总体的一个比例

![Diagram illustrating lab1](E:\SimpleDB\笔记\Diagram illustrating lab1.png)

也就是说，我们需要去计算一个选择率

```
步骤：
1、首先需要把值传进来放入 buckets
2、然后根据传进来的运算符，计算值所占的比例
3、计算的过程
	首先统计当前值 value 前的所有总数，也就是通过buckets[] 去统计所有的总数
	然后就去计算value所占的数量，其实就是计算面积，index 就是 value 所在的索引柱
	buckets[index] / width 计算出当前柱的高度
	v - index * width - min 计算当前柱所占的宽度
	然后对面积进行计算 (1.0 * buckets[index] / width) * (v - index * width - min)
	这里 1.0 的目的是 避免精度丢失
```

### 参数

- private int[] buckets;  // 直方图
- private int min;  // 边界最小值
- private int max;   // 边界最大值
- private double width;  // 长度
- private int tuplesCount = 0;  // 行数

### 方法

- addValue(int v) ：添加新的值

  ```java
  private int getIndex(int v){
      return (int) ((v - min) / width);
  }
  public void addValue(int v) {
      // some code goes here
      if(v >= min && v <= max){
          buckets[getIndex(v)]++;
          tuplesCount++;
      }
  }
  ```

- double estimateSelectivity(Predicate.Op op, int v) ： 根据运算符和值 估计选择率

  ```java
  public double estimateSelectivity(Predicate.Op op, int v) {
      // some code goes here
      switch (op){
          case LESS_THAN:
              if(v <= min){
                  return 0.0;
              }
              else if(v >= max){
                  return 1.0;
              }
              else{
                  int index = getIndex(v);
                  double tuples = 0;
                  for (int i = 0; i < index; i++) {
                      tuples += buckets[i];
                  }
                  // 索引所在柱的高度 * （当前值 - 该柱前的宽度）<这个也就是当前柱所占的宽度>
                  tuples += (1.0 * buckets[index] / width) * (v - index * width - min);
                  return tuples / tuplesCount;
              }
          case GREATER_THAN:
              return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
          case EQUALS:
              return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
          case NOT_EQUALS:
              return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
          case GREATER_THAN_OR_EQ:
              return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
          case LESS_THAN_OR_EQ:
              return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
          default:
              throw new IllegalArgumentException("Operation is illegal");
      }
  }
  ```

### 全代码

```java
package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    // 直方图
    private int[] buckets;
    // 边界最小值
    private int min;
    // 边界最大值
    private int max;
    // 长度
    private double width;
    // 行数
    private int tuplesCount = 0;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (max - min + 1.0) / buckets;
        this.tuplesCount = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    private int getIndex(int v){
        return (int) ((v - min) / width);
    }
    public void addValue(int v) {
    	// some code goes here
        if(v >= min && v <= max){
            buckets[getIndex(v)]++;
            tuplesCount++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        switch (op){
            case LESS_THAN:
                if(v <= min){
                    return 0.0;
                }
                else if(v >= max){
                    return 1.0;
                }
                else{
                    int index = getIndex(v);
                    double tuples = 0;
                    for (int i = 0; i < index; i++) {
                        tuples += buckets[i];
                    }
                    // 索引所在柱的高度 * （当前值 - 该柱前的宽度）<这个也就是当前柱所占的宽度>
                    tuples += (1.0 * buckets[index] / width) * (v - index * width - min);
                    return tuples / tuplesCount;
                }
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            case EQUALS:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            default:
                throw new IllegalArgumentException("Operation is illegal");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        int cnt = 0;
        for(int bucket : buckets){
                cnt += bucket;
        }
        if(cnt == 0) return 0;
        return (cnt * 1.0 / tuplesCount);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("IntHistogram(buckets = %d, min = %d, max = %d)", buckets.length, min, max);
    }
}
```

## StringHistogram

这个其实就是套用 IntHistogram，只是书写了一个字符串转换为数字的写法

### 参数

- final IntHistogram hist;  // 套用数字直方图

### 方法

- stringToInt(String s) ：将字符串转化为数字，要注意最大值和最小值

  ```java
  private int stringToInt(String s) {
      int i;
      int v = 0;
      for (i = 3; i >= 0; i--) {
          if (s.length() > 3 - i) {
              int ci = s.charAt(3 - i);
              v += (ci) << (i * 8);
          }
      }
  
      // XXX: hack to avoid getting wrong results for
      // strings which don't output in the range min to max
      if (!(s.equals("") || s.equals("zzzz"))) {
          if (v < minVal()) {
              v = minVal();
          }
  
          if (v > maxVal()) {
              v = maxVal();
          }
      }
  
      return v;
  }
  ```

- maxVal() 和 minVal()

  ```java
  int maxVal() {
      return stringToInt("zzzz");
  }
  int minVal() {
      return stringToInt("");
  }
  ```

## 测试

IntHistogramTest



# Exercise 2

## TableStats

记录多个表和相应表内的每个字段的相应的直方图

### 参数

- ConcurrentMap<String, TableStats> statsMap；// 每个表直方图的缓存
- static final int IOCOSTPERPAGE = 1000;  // 每个页面的 IO 成本
- static final int NUM_HIST_BINS = 100;    // 每个直方图应该分的段数
- private int ioCostPerPage;  // 读取每个页的 IO 成本
- private DbFile dbFile;   // 需要进行数据统计的表
- private TupleDesc tupleDesc;  // 表的属性行
- private int tableid;  // 表id
- private int pagesNum;  // 一个表中页的数量
- private int tupleNum;  // 一页中行的数量
- private int fieldNum;  // 一行中段的数量
- HashMap<Integer, IntHistogram> integerHashMap;  // 第 i 个整型字段 和 第 i 个直方图的映射
- HashMap<Integer, StringHistogram> stringHashMap;  // 第 i 个字符串字段 和 第 i 个直方图的映射

### 方法

- TableStats(int tableid, int ioCostPerPage)：填充所有参数

  - 先查询每行，通过SeqScan，然后在根据每段建立直方图，只需要将行迭代器重置，遍历每个段

  ```java
  public TableStats(int tableid, int ioCostPerPage) {
      // 基本的设置
      tupleNum = 0;
      this.tableid = tableid;
      this.ioCostPerPage = ioCostPerPage;
      this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
      this.pagesNum = ((HeapFile) dbFile).numPages();
      integerHashMap = new HashMap<>();
      stringHashMap = new HashMap<>();
  
      // 获取字段数
      this.tupleDesc = dbFile.getTupleDesc();
      this.fieldNum = tupleDesc.numFields();
  
      // 获得行数
      Type[] types = getTypes(tupleDesc);
      int[] mins = new int[fieldNum];
      int[] maxs = new int[fieldNum];
  
      // 根据表查询行
      TransactionId tid = new TransactionId();
      // 查询每一行
      SeqScan scan = new SeqScan(tid, tableid);
      // 统计数字字段的最大值和最小值
      try{
          // 打开迭代器
          scan.open();
          // 获取每个字段的最小值和最大值，一共有 fieldNum个字段
          for (int i = 0; i < fieldNum; i++) {
              // 如果是字符串，跳过
              if(types[i] == Type.STRING_TYPE){
                  continue;
              }
              int min = Integer.MAX_VALUE;
              int max = Integer.MIN_VALUE;
              while(scan.hasNext()){
                  if(i == 0){
                      tupleNum++;
                  }
                  Tuple tuple = scan.next();
                  IntField field = (IntField)tuple.getField(i);
                  int val = field.getValue();
                  max = Math.max(val, max);
                  min = Math.min(val, min);
              }
              // 迭代器重置
              scan.rewind();
              mins[i] = min;
              maxs[i] = max;
          }
      }catch (Exception e){
          e.printStackTrace();
      }finally {
          scan.close();
      }
  
      // 写入缓存map中
      for (int i = 0; i < fieldNum; i++) {
          // 如果是整数，创立整数直方图
          if(types[i] == Type.INT_TYPE){
              integerHashMap.put(i, new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]));
          }
          else{
              stringHashMap.put(i, new StringHistogram(NUM_HIST_BINS));
          }
      }
  
      // 把相应的值填入直方图
      addValueToHist();
  }
  
  /**
       * 获取相应的类型
       * */
  private Type[] getTypes(TupleDesc td){
      int numField = td.numFields();
      Type[] types = new Type[numField];
      for (int i = 0; i < numField; i++) {
          Type t = td.getFieldType(i);
          types[i] = t;
      }
      return types;
  }
  
  /**
       * 把相应的值填入直方图
       * */
  private void addValueToHist(){
      TransactionId tid = new TransactionId();
      SeqScan scan = new SeqScan(tid, tableid);
      try{
          // 打开迭代器
          scan.open();
          // 获取每行的各个字段值写入
          while(scan.hasNext()){
              Tuple tuple = scan.next();
              // 遍历每个字段
              for (int i = 0; i < fieldNum; i++) {
                  Field field = tuple.getField(i);
                  // 判断类型
                  if(field.getType() == Type.INT_TYPE){
                      integerHashMap.get(i).addValue(((IntField) field).getValue());
                  }
                  else{
                      stringHashMap.get(i).addValue(((StringField) field).getValue());
                  }
              }
          }
      }catch (Exception e){
          e.printStackTrace();
      }finally {
          scan.close();
      }
  }
  ```

### 全代码

```java
package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    // 每个表直方图的缓存
    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * number of bins for the histogram. feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    // 每个直方图应该分的段数
    static final int NUM_HIST_BINS = 100;
    // 读取每个页的 IO 成本
    private int ioCostPerPage;
    // 需要进行数据统计的表
    private DbFile dbFile;
    // 表的属性行
    private TupleDesc tupleDesc;
    // 表id
    private int tableid;
    // 一个表中页的数量
    private int pagesNum;
    // 一页中行的数量
    private int tupleNum;
    // 一行中段的数量
    private int fieldNum;
    // 第 i 个整型字段 和 第 i 个直方图的映射
    private HashMap<Integer, IntHistogram> integerHashMap;
    // 第 i 个字符串字段 和 第 i 个直方图的映射
    private HashMap<Integer, StringHistogram> stringHashMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

        // 基本的设置
        tupleNum = 0;
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.pagesNum = ((HeapFile) dbFile).numPages();
        integerHashMap = new HashMap<>();
        stringHashMap = new HashMap<>();

        // 获取字段数
        this.tupleDesc = dbFile.getTupleDesc();
        this.fieldNum = tupleDesc.numFields();

        // 获得行数
        Type[] types = getTypes(tupleDesc);
        int[] mins = new int[fieldNum];
        int[] maxs = new int[fieldNum];

        // 根据表查询行
        TransactionId tid = new TransactionId();
        // 查询每一行
        SeqScan scan = new SeqScan(tid, tableid);
        // 统计数字字段的最大值和最小值
        try{
            // 打开迭代器
            scan.open();
            // 获取每个字段的最小值和最大值，一共有 fieldNum个字段
            for (int i = 0; i < fieldNum; i++) {
                // 如果是字符串，跳过
                if(types[i] == Type.STRING_TYPE){
                    continue;
                }
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                while(scan.hasNext()){
                    if(i == 0){
                        tupleNum++;
                    }
                    Tuple tuple = scan.next();
                    IntField field = (IntField)tuple.getField(i);
                    int val = field.getValue();
                    max = Math.max(val, max);
                    min = Math.min(val, min);
                }
                // 迭代器重置
                scan.rewind();
                mins[i] = min;
                maxs[i] = max;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            scan.close();
        }

        // 写入缓存map中
        for (int i = 0; i < fieldNum; i++) {
            // 如果是整数，创立整数直方图
            if(types[i] == Type.INT_TYPE){
                integerHashMap.put(i, new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]));
            }
            else{
                stringHashMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        // 把相应的值填入直方图
        addValueToHist();
    }

    /**
     * 获取相应的类型
     * */
    private Type[] getTypes(TupleDesc td){
        int numField = td.numFields();
        Type[] types = new Type[numField];
        for (int i = 0; i < numField; i++) {
            Type t = td.getFieldType(i);
            types[i] = t;
        }
        return types;
    }

    /**
     * 把相应的值填入直方图
     * */
    private void addValueToHist(){
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, tableid);
        try{
            // 打开迭代器
            scan.open();
            // 获取每行的各个字段值写入
            while(scan.hasNext()){
                Tuple tuple = scan.next();
                // 遍历每个字段
                for (int i = 0; i < fieldNum; i++) {
                    Field field = tuple.getField(i);
                    // 判断类型
                    if(field.getType() == Type.INT_TYPE){
                        integerHashMap.get(i).addValue(((IntField) field).getValue());
                    }
                    else{
                        stringHashMap.get(i).addValue(((StringField) field).getValue());
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            scan.close();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return pagesNum * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // 选择因子 * 行的数量，得出来应该选择多少行
        double cardinality = selectivityFactor * tupleNum;
        return (int) cardinality ;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        // 判断类型
        if(tupleDesc.getFieldType(field) == Type.INT_TYPE){
            return integerHashMap.get(field).avgSelectivity();
        }
        else{
            return stringHashMap.get(field).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        // 判断类型
        if(tupleDesc.getFieldType(field) == Type.INT_TYPE){
            return integerHashMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }
        else{
            return stringHashMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNum;
    }

}
```

## 测试

TableStatsTest



# Exercise 3

## JoinOptimizer

### 方法

- estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2)
  - 估计花费的成本
  - 公式 =  IO 成本（表1 检索成本 + 每一行都需要检索 表2） + CPU成本（总共的扫描次数）

```java
public double estimateJoinCost(LogicalJoinNode j, int card1, int card2,
        double cost1, double cost2) {
    if (j instanceof LogicalSubplanJoinNode) {
        // A LogicalSubplanJoinNode represents a subquery.
        // You do not need to implement proper support for these for Lab 3.
        return card1 + cost1 + cost2;
    } else {
        // Insert your code here.
        // HINT: You may need to use the variable "j" if you implemented
        // a join algorithm that's more complicated than a basic
        // nested-loops join.
        // 成本是 ： IO 成本（表1 检索成本 + 每一行都需要检索 表2） + CPU成本（总共的扫描次数）
        double cost = cost1 + card1 * cost2 + card1 * card2;
        return cost;
    }
}
```

- estimateTableJoinCardinality(...)：计算两个表的连接基数

  - 从讲义中可以看到，当相等的情况下，都是主键的情况下，取最小的主键，都是非主键的情况下，取最大的非主键，一非一主的时候，取非主键
  - 当不是相等运算符号的时候，取 30% * card1 * card2 

  ```java
  public static int estimateTableJoinCardinality(Predicate.Op joinOp, String table1Alias, String table2Alias, String field1PureName, String field2PureName, int card1, int card2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats, Map<String, Integer> tableAliasToId) {
      int card = 1;
      // some code goes here
      if(joinOp == Predicate.Op.EQUALS){
          // 取非主键
          if(t1pkey && !t2pkey){
              card = card2;
          }
          else if(!t1pkey && t2pkey){
              card = card1;
          }
          // 两个非主键，取最大
          else if(!t1pkey && !t2pkey){
              card = Math.max(card1, card2);
          }
          // 两个主键，取最小
          else{
              card = Math.min(card1, card2);
          }
      }
      else{
          // 如果不是等于的情况下，是 30%
          card = (int)(0.3 * card1 * card2);
      }
      return card <= 0 ? 1 : card;
  }
  ```

  

## 测试

通过 `JoinOptimizerTest.java` 中的estimateJoinCostTest 和 estimateJoinCardinality，其他的暂时不用管



# Exercise 4

## CostCard

### 参数

- public double cost;    //该连接顺序下的代价
- public int card;    //该连接顺序下的基数
- public List<LogicalJoinNode> plan;    //按照某一顺序连接的查询计划

## PageCache

### 参数

- final Map<Set<LogicalJoinNode>,List<LogicalJoinNode>> bestOrders	// 最佳计划
- final Map<Set<LogicalJoinNode>,Double> bestCosts								      // 最佳花费
- final Map<Set<LogicalJoinNode>,Integer> bestCardinalities                          // 最佳基数

## JoinOptimizer

### 方法

- enumerateSubsets(List<T> v, int size)：辅助方法
  - 枚举给定子集的所有大小

```java
public <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
    Set<Set<T>> els = new HashSet<>();
    els.add(new HashSet<>());
    // Iterator<Set> it;
    // long start = System.currentTimeMillis();

    for (int i = 0; i < size; i++) {
        Set<Set<T>> newels = new HashSet<>();
        for (Set<T> s : els) {
            for (T t : v) {
                Set<T> news = new HashSet<>(s);
                if (news.add(t))
                    newels.add(news);
            }
        }
        els = newels;
    }

    return els;

}
```

- printJoins()：将连接计划进行图形展示（基于Swing）
- private CostCard computeCostAndCardOfSubplan：计算子查询的查询代价

- orderJoins(...)：在指定表上计算一个有效合理的逻辑连接

```java
public List<LogicalJoinNode> orderJoins(
    Map<String, TableStats> stats,
    Map<String, Double> filterSelectivities, boolean explain)
    throws ParsingException {

    // some code goes here
    //Replace the following
    CostCard bestCostCard = new CostCard();
    PlanCache planCache = new PlanCache();
    int size = joins.size();
    for(int i = 1; i <= size; i++){
        // 枚举当前子集的所有大小
        Set<Set<LogicalJoinNode>> subSets = enumerateSubsets(joins, i);
        for(Set<LogicalJoinNode> subSet : subSets){
            // 最佳花费
            double bestCostSoFar = Double.MAX_VALUE;
            bestCostCard = new CostCard();
            for (LogicalJoinNode removeJoinNode : subSet) {
                // 计算查询子代价
                CostCard costCard = computeCostAndCardOfSubplan(stats, filterSelectivities, removeJoinNode, subSet, bestCostSoFar, planCache);
                if (costCard != null) {
                    bestCostSoFar = costCard.cost;
                    bestCostCard = costCard;
                }
            }
            // 如果被修改，说明有最佳
            if(bestCostSoFar != Double.MAX_VALUE){
                planCache.addPlan(subSet, bestCostCard.cost, bestCostCard.card, bestCostCard.plan);
            }
        }
        // 是否打印图形化计划
        if(explain){
            printJoins(bestCostCard.plan, planCache, stats, filterSelectivities);
        }
    }

    return bestCostCard.plan;
}
```

