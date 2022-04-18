[toc]

# Exercise 1

## Predicate 单比较

一个元组的某个字段是否满足对应条件

### 参数

- private final int field  要比较的字段数

- private final Op op   比较符号

  - 内部类

    ```
    EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;
    ```

- private Field operand    元组传递的比较字段

  - 一般新建一个 IntField 用来比较

### 方法

- filter(Tuple t)  比较传进来的元组对应字段

  ```java
  public boolean filter(Tuple t) {
      // some code goes here
      // 获取字段，传入比较符号，和当前 Field比较
      return t.getField(field).compare(op, operand);
  }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    private final int field;
    private final Op op;
    private Field operand;
    
    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // some code goes here
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // some code goes here
        // 获取字段，传入比较符号，和当前 Field比较
        return t.getField(field).compare(op, operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
        return "f = " + field + " op = " + op.toString() + " operand = " + operand.toString() ;
    }
}

```

## JoinPredicate 双向比较

两个元组内的字段比较，主要用于 Join 运算符

### 参数

- private final int field1;	字段一
- private final Predicate.Op op;     比较符号
- private final int field2;    字段二

### 方法

- filter(Tuple t1, Tuple t2)   两个元组进行比较

  ```java
      public boolean filter(Tuple t1, Tuple t2) {
          // some code goes here
          return t1.getField(field1).compare(op, t2.getField(field2));
      }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int field1;
    private final Predicate.Op op;
    private final int field2;



    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.field1 = field1;
        this.op = op;
        this.field2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        return t1.getField(field1).compare(op, t2.getField(field2));
    }
    
    public int getField1()
    {
        // some code goes here
        return field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}

```



## Filter 过滤条件

也就是相当于 where 语句后的 条件限定

### 参数

- private final Predicate predicate;
- private OpIterator child;

### 方法

- Tuple fetchNext()   比较是否符号条件，使用 filter过滤

  ```java
  protected Tuple fetchNext() throws NoSuchElementException,
  TransactionAbortedException, DbException {
      // some code goes here
      while(child.hasNext()){
          Tuple tuple = child.next();
          if(predicate.filter(tuple)){
              return tuple;
          }
      }
      return null;
  }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate predicate;
    private OpIterator child;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while(child.hasNext()){
            Tuple tuple = child.next();
            if(predicate.filter(tuple)){
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
```

## Join 连接

自然连接，无去重

### 参数

- private final JoinPredicate joinPredicate;    双元组比较
- private OpIterator child1;     元组迭代器1
- private OpIterator child2;     元组迭代器2
- private Tuple t;    临时元组，保存上次迭代用的 child1 的 Tuple
  - 这个主要用于下次遍历的时候，继续使用当前元组，因为可能有多个符合条件的结果，也就是一对多，所以不能够直接跳到下一条，使用临时值保存当前元组

### 方法

- public TupleDesc getTupleDesc()   返回的是合并后的 属性组

  ```java
  public TupleDesc getTupleDesc() {
      // some code goes here
      return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
  }
  ```

- public void rewind()  重置

  ```java
  public void rewind() throws DbException, TransactionAbortedException {
      // some code goes here
      child1.rewind();
      child2.rewind();
      t = null;
  }
  ```

- Tuple fetchNext()   寻找符合条件的 tuple

  ```java
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
      // some code goes here
      // t 的意义就是 笛卡尔积, 一个可能对多个相等
      while (child1.hasNext() || t != null){
          if(child1.hasNext() && t == null){
              t = child1.next();
          }
          while(child2.hasNext()){
              Tuple t2 = child2.next();
              if(joinPredicate.filter(t, t2)){
                  TupleDesc td1 = t.getTupleDesc();
                  TupleDesc td2 = t2.getTupleDesc();
                  // 合并
                  TupleDesc tupleDesc = TupleDesc.merge(td1, td2);
                  // 创建新的行
                  Tuple newTuple = new Tuple(tupleDesc);
                  // 设置路径
                  newTuple.setRecordId(t.getRecordId());
                  // 合并
                  int i = 0;
                  for (; i < td1.numFields(); i++) {
                      newTuple.setField(i, t.getField(i));
                  }
                  for (int j = 0; j < td2.numFields(); j++) {
                      newTuple.setField(i + j, t2.getField(j));
                  }
                  // 遍历完t2后重置，t置空，准备遍历下一个
                  if(!child2.hasNext()){
                      child2.rewind();
                      t = null;
                  }
                  return newTuple;
              }
          }
          // 重置 child2
          child2.rewind();
          t = null;
      }
      return null;
  }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.common.Type;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private final JoinPredicate joinPredicate;
    private OpIterator child1;
    private OpIterator child2;
    // 临时元组，保存上次迭代用的 child1 的 Tuple
    private Tuple t;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(joinPredicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(joinPredicate.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child2.close();
        child1.close();

    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
        t = null;
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // t 的意义就是 笛卡尔积, 一个可能对多个相等
        while (child1.hasNext() || t != null){
            if(child1.hasNext() && t == null){
                t = child1.next();
            }
            while(child2.hasNext()){
                Tuple t2 = child2.next();
                if(joinPredicate.filter(t, t2)){
                    TupleDesc td1 = t.getTupleDesc();
                    TupleDesc td2 = t2.getTupleDesc();
                    // 合并
                    TupleDesc tupleDesc = TupleDesc.merge(td1, td2);
                    // 创建新的行
                    Tuple newTuple = new Tuple(tupleDesc);
                    // 设置路径
                    newTuple.setRecordId(t.getRecordId());
                    // 合并
                    int i = 0;
                    for (; i < td1.numFields(); i++) {
                        newTuple.setField(i, t.getField(i));
                    }
                    for (int j = 0; j < td2.numFields(); j++) {
                        newTuple.setField(i + j, t2.getField(j));
                    }
                    // 遍历完t2后重置，t置空，准备遍历下一个
                    if(!child2.hasNext()){
                        child2.rewind();
                        t = null;
                    }
                    return newTuple;
                }
            }
            // 重置 child2
            child2.rewind();
            t = null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
    }

}
```

## 测试

PredicateTest,   JoinPredicateTest,  FilterTest,  and JoinTest

# Exercise 2

## IntegerAggregator

数字字段聚合

### 参数

- private final int gbfield;
  - // 分组字段的序号（是一个字段，用于辨别是否是该类型）举例：group by 字段

- private final Type gbfieldtype;   //分组字段的类型

- private int afield;   // 聚合字段的序号（是用于取新插入的值） 举例： sum(字段),min(字段)

- private Op what;   // 操作符

- private AggHandler aggHandler;
  - AggHandler 自定义内部抽象类
  
    ```java
    private abstract class AggHandler{
        // 存储字段对应的聚合结果
        Map<Field, Integer> aggResult;
        // gbField 用于分组的字段， aggField 现阶段聚合结果
        abstract void handle(Field gbField, IntField aggField);
    
        public AggHandler(){
            aggResult = new HashMap<>();
        }
    
        public Map<Field, Integer> getAggResult() {
            return aggResult;
        }
    }
    ```
  
    - aggResult 对应的是每个字段的聚合结果
    - 比如 field1 字段的最小值，那么 key 就是 field1，value 就是 最小值，每次取出来比较
  
  - 抽象类：对应多个子类实现，CountHandler、SumHandler、MaxHandler、MinHandler、AvgHandler

### 方法

- IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what)  构造函数

  ```java
  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
      // some code goes here
      this.gbfield = gbfield;
      this.gbfieldtype = gbfieldtype;
      this.afield = afield;
      this.what = what;
      // 判读运算符号
      switch (what){
          case MIN:
              aggHandler = new MinHandler();
              break;
          case MAX:
              aggHandler = new MaxHandler();
              break;
          case AVG:
              aggHandler = new AvgHandler();
              break;
          case SUM:
              aggHandler = new SumHandler();
              break;
          case COUNT:
              aggHandler = new CountHandler();
              break;
          default:
              throw new IllegalArgumentException("聚合器不支持当前运算符");
      }
  }
  ```

- mergeTupleIntoGroup(Tuple tup)   分组并聚合到aggregate的 aggResult

  ```java
  public void mergeTupleIntoGroup(Tuple tup) {
      // some code goes here
      // 获得要处理值的字段
      IntField afield = (IntField) tup.getField(this.afield);
      // 分组的字段
      Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
      aggHandler.handle(gbfield, afield);
  }
  ```

- OpIterator iterator()  返回结果迭代器

  - 这里的结果是每一个被分组的字段聚合后的结果返回

  ```java
  public OpIterator iterator() {
      // some code goes here
      // 获取聚合集
      Map<Field, Integer> aggResult = aggHandler.getAggResult();
      // 构建 tuple 需要
      Type[] types;
      String[] names;
      TupleDesc tupleDesc;
      // 储存结果
      List<Tuple> tuples = new ArrayList<>();
      // 如果没有分组
      if(gbfield == NO_GROUPING){
          types = new Type[]{Type.INT_TYPE};
          names = new String[]{"aggregateVal"};
          tupleDesc = new TupleDesc(types, names);
          // 获取结果字段
          IntField resultField = new IntField(aggResult.get(null));
          // 组合成行（临时行，不需要存储，只需要设置字段值）
          Tuple tuple = new Tuple(tupleDesc);
          tuple.setField(0, resultField);
          tuples.add(tuple);
      }
      else{
          types = new Type[]{gbfieldtype, Type.INT_TYPE};
          names = new String[]{"groupVal", "aggregateVal"};
          tupleDesc = new TupleDesc(types, names);
          for(Field field: aggResult.keySet()){
              Tuple tuple = new Tuple(tupleDesc);
              if(gbfieldtype == Type.INT_TYPE){
                  IntField intField = (IntField) field;
                  tuple.setField(0, intField);
              }
              else{
                  StringField stringField = (StringField) field;
                  tuple.setField(0, stringField);
              }
  
              IntField resultField = new IntField(aggResult.get(field));
              tuple.setField(1, resultField);
              tuples.add(tuple);
          }
      }
      return new TupleIterator(tupleDesc ,tuples);
  }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // 分组字段的序号（是一个字段，用于辨别是否是该类型）举例：group by 字段
    private final int gbfield;
    private final Type gbfieldtype;
    // 聚合字段的序号（是用于取新插入的值） 举例： sum(字段),min(字段)
    private int afield;
    // 操作符
    private Op what;

    private AggHandler aggHandler;

    private abstract class AggHandler{
        // 存储字段对应的聚合结果
        Map<Field, Integer> aggResult;
        // gbField 用于分组的字段， aggField 现阶段聚合结果
        abstract void handle(Field gbField, IntField aggField);

        public AggHandler(){
            aggResult = new HashMap<>();
        }

        public Map<Field, Integer> getAggResult() {
            return aggResult;
        }
    }

    private class CountHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, aggResult.get(gbField) + 1);
            }
            else{
                aggResult.put(gbField, 1);
            }
        }
    }

    private class SumHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField, aggResult.get(gbField) + value);
            }
            else{
                aggResult.put(gbField, value);
            }
        }
    }

    private class MaxHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField,Math.max(aggResult.get(gbField), value));
            }
            else{
                aggResult.put(gbField, value);
            }
        }
    }

    private class MinHandler extends AggHandler{
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            if(aggResult.containsKey(gbField)){
                aggResult.put(gbField,Math.min(aggResult.get(gbField), value));
            }
            else{
                aggResult.put(gbField, value);
            }
        }
    }

    private class AvgHandler extends AggHandler{
        Map<Field, Integer> sum = new HashMap<>();
        Map<Field, Integer> count = new HashMap<>();
        @Override
        void handle(Field gbField, IntField aggField) {
            int value = aggField.getValue();
            // 求和 + 计数
            if(sum.containsKey(gbField) && count.containsKey(gbField)){
                sum.put(gbField, sum.get(gbField) + value);
                count.put(gbField, count.get(gbField) + 1);
            }
            else{
                sum.put(gbField, value);
                count.put(gbField, 1);
            }
            aggResult.put(gbField, sum.get(gbField) / count.get(gbField));
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        // 判读运算符号
        switch (what){
            case MIN:
                aggHandler = new MinHandler();
                break;
            case MAX:
                aggHandler = new MaxHandler();
                break;
            case AVG:
                aggHandler = new AvgHandler();
                break;
            case SUM:
                aggHandler = new SumHandler();
                break;
            case COUNT:
                aggHandler = new CountHandler();
                break;
            default:
                throw new IllegalArgumentException("聚合器不支持当前运算符");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated(指示) in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // 获得要处理值的字段
        IntField afield = (IntField) tup.getField(this.afield);
        // 分组的字段
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        aggHandler.handle(gbfield, afield);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // 获取聚合集
        Map<Field, Integer> aggResult = aggHandler.getAggResult();
        // 构建 tuple 需要
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        // 储存结果
        List<Tuple> tuples = new ArrayList<>();
        // 如果没有分组
        if(gbfield == NO_GROUPING){
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            // 获取结果字段
            IntField resultField = new IntField(aggResult.get(null));
            // 组合成行（临时行，不需要存储，只需要设置字段值）
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, resultField);
            tuples.add(tuple);
        }
        else{
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            names = new String[]{"groupVal", "aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            for(Field field: aggResult.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                if(gbfieldtype == Type.INT_TYPE){
                    IntField intField = (IntField) field;
                    tuple.setField(0, intField);
                }
                else{
                    StringField stringField = (StringField) field;
                    tuple.setField(0, stringField);
                }

                IntField resultField = new IntField(aggResult.get(field));
                tuple.setField(1, resultField);
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc ,tuples);
    }

}
```

## StringAggregator

和 IntegerAggerator 差不多，差别是这边只支持计数 count 的功能

### 参数

- private final int gbfield;
- private final Type gbfieldtype;
- private final int afield;
- private final Op what;
- Map<Field, Integer> aggResult;

### 方法

- mergeTupleIntoGroup(Tuple tup)  合并到元组

  ```java
  public void mergeTupleIntoGroup(Tuple tup) {
      // some code goes here
      // 分组
      Field gbFiled = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
      // 聚合值 由于是字符串，这里是计数，没有任何使用
      //StringField aField = (StringField) tup.getField(afield);
      //String newValue = aField.getValue();
      if(aggResult.containsKey(gbFiled)){
          aggResult.put(gbFiled, aggResult.get(gbFiled) + 1);
      }
      else{
          aggResult.put(gbFiled, 1);
      }
  }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    Map<Field, Integer> aggResult;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(!what.equals(Op.COUNT)){
            throw new IllegalArgumentException("String类型只支持计数");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        aggResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // 分组
        Field gbFiled = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        // 聚合值 由于是字符串，这里是计数，没有任何使用
        //StringField aField = (StringField) tup.getField(afield);
        //String newValue = aField.getValue();
        if(aggResult.containsKey(gbFiled)){
            aggResult.put(gbFiled, aggResult.get(gbFiled) + 1);
        }
        else{
            aggResult.put(gbFiled, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // 构建 tuple 需要
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        // 储存结果
        List<Tuple> tuples = new ArrayList<>();
        if(gbfield == NO_GROUPING){
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(aggResult.get(null)));
            tuples.add(tuple);
        }else{
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            names = new String[]{"groupVal", "aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            for(Field field: aggResult.keySet()){
                Tuple tuple = new Tuple(tupleDesc);

                if(gbfieldtype == Type.INT_TYPE){
                    IntField intField = (IntField) field;
                    tuple.setField(0, intField);
                }
                else{
                    StringField stringField = (StringField) field;
                    tuple.setField(0, stringField);
                }

                IntField resultField = new IntField(aggResult.get(field));
                tuple.setField(1, resultField);
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
```

## Aggregate

实现分组聚合，就是调用上面两个聚合计算器

### 参数

```java
    // 需要聚合的 tuples
    private OpIterator child;
    // 聚合字段
    private final int afield;
    // 分组字段
    private final int gfield;
    // 运算符
    private Aggregator.Op aop;

    // 进行聚合操作的类
    private Aggregator aggregator;
    // 聚合结果的迭代器
    private OpIterator opIterator;
    // 聚合结果的属性行
    private TupleDesc tupleDesc;
```

### 方法

-  Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop)  构造器

  ```java
  public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
      // some code goes here
      this.child = child;
      this.afield = afield;
      this.gfield = gfield;
      this.aop = aop;
      // 判断是否分组
      Type gfieldtype = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);
  
      // 创建聚合器
      if(child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE){
          this.aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
      }
      else{
          this.aggregator = new IntegerAggregator(gfield, gfieldtype, afield, aop);
      }
  
      // 组建 TupleDesc
      List<Type> typeList = new ArrayList<>();
      List<String> nameList = new ArrayList<>();
  
      if(gfieldtype != null){
          typeList.add(gfieldtype);
          nameList.add(child.getTupleDesc().getFieldName(gfield));
      }
  
      typeList.add(child.getTupleDesc().getFieldType(afield));
      nameList.add(child.getTupleDesc().getFieldName(afield));
  
      if(aop.equals(Aggregator.Op.SUM_COUNT)){
          typeList.add(Type.INT_TYPE);
          nameList.add("COUNT");
      }
      this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
  }
  ```

- open()，打开的时候处理所有的 child 变成聚合结果字段

  ```java
  public void open() throws NoSuchElementException, DbException,
  TransactionAbortedException {
      // some code goes here
      // 聚合所有的tuple
      child.open();
      while(child.hasNext()){
          aggregator.mergeTupleIntoGroup(child.next());
      }
      // 获取聚合后的迭代器
      opIterator = aggregator.iterator();
      // 查询
      opIterator.open();
      // 使父类状态保持一致
      super.open();
  }
  ```

- setChildren(OpIterator[] children)  设置初始字段，相当于二次设置，因此需要更新类型组

  ```java
  public void setChildren(OpIterator[] children) {
      // some code goes here
      this.child = children[0];
      Type gfieldtype = child.getTupleDesc().getFieldType(gfield);
  
      // 组建 TupleDesc
      List<Type> typeList = new ArrayList<>();
      List<String> nameList = new ArrayList<>();
  
      // 加入分组后的字段
      if(gfieldtype != null){
          typeList.add(gfieldtype);
          nameList.add(child.getTupleDesc().getFieldName(gfield));
      }
  
      // 加入聚合字段
      typeList.add(child.getTupleDesc().getFieldType(afield));
      nameList.add(child.getTupleDesc().getFieldName(afield));
  
      if(aop.equals(Aggregator.Op.SUM_COUNT)){
          typeList.add(Type.INT_TYPE);
          nameList.add("COUNT");
      }
  
      this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
  }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    // 需要聚合的 tuples
    private OpIterator child;
    // 聚合字段
    private final int afield;
    // 分组字段
    private final int gfield;
    // 运算符
    private Aggregator.Op aop;

    // 进行聚合操作的类
    private Aggregator aggregator;
    // 聚合结果的迭代器
    private OpIterator opIterator;
    // 聚合结果的属性行
    private TupleDesc tupleDesc;


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        // 判断是否分组
        Type gfieldtype = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);

        // 创建聚合器
        if(child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE){
            this.aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
        }
        else{
            this.aggregator = new IntegerAggregator(gfield, gfieldtype, afield, aop);
        }

        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        if(gfieldtype != null){
            typeList.add(gfieldtype);
            nameList.add(child.getTupleDesc().getFieldName(gfield));
        }

        typeList.add(child.getTupleDesc().getFieldType(afield));
        nameList.add(child.getTupleDesc().getFieldName(afield));

        if(aop.equals(Aggregator.Op.SUM_COUNT)){
            typeList.add(Type.INT_TYPE);
            nameList.add("COUNT");
        }
        this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName(){
        // some code goes here

        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        if(gfield == -1){
            return tupleDesc.getFieldName(0);
        }
        return tupleDesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        // 聚合所有的tuple
        child.open();
        while(child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        // 获取聚合后的迭代器
        opIterator = aggregator.iterator();
        // 查询
        opIterator.open();
        // 使父类状态保持一致
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(opIterator.hasNext()){
            return opIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        opIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
        Type gfieldtype = child.getTupleDesc().getFieldType(gfield);

        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        // 加入分组后的字段
        if(gfieldtype != null){
            typeList.add(gfieldtype);
            nameList.add(child.getTupleDesc().getFieldName(gfield));
        }

        // 加入聚合字段
        typeList.add(child.getTupleDesc().getFieldType(afield));
        nameList.add(child.getTupleDesc().getFieldName(afield));

        if(aop.equals(Aggregator.Op.SUM_COUNT)){
            typeList.add(Type.INT_TYPE);
            nameList.add("COUNT");
        }

        this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
    }
}
```

## 测试

 IntegerAggregatorTest, StringAggregatorTest, and AggregateTest.

# Exercise 3

## HeapPage

在之前的基础上修改

### 参数

- private TransactionId tid;   事务 id
- private boolean dirty;   是否是脏页

### 全代码

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

    private final HeapPageId pid;
    private final TupleDesc td;
    // 槽储存
    private final byte[] header;
    // 元组数据
    private final Tuple[] tuples;
    // 槽数
    private final int numSlots;

    // 事务 id
    private TransactionId tid;
    // 是否是脏页
    private boolean dirty;

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
        // 查看属性是否匹配
        int tupleId = t.getRecordId().getTupleNumber();
        // 页面已被删除， 类型不相同， 页面不相同
        if(tuples[tupleId] == null || !t.getTupleDesc().equals(td) || !t.getRecordId().getPageId().equals(pid)){
            throw new DbException("this tuple is not on this page");
        }
        if(!isSlotUsed(tupleId)){
            throw new DbException("tuple slot is already empty");
        }
        // 标记未被使用
        markSlotUsed(tupleId, false);
        // 删除插槽内容
        tuples[tupleId] = null;
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
        // 查看 Page 是否已满
       if(getNumEmptySlots() == 0){
           throw new DbException("当前页已满");
       }

       // 查看属性是否匹配
        if(!t.getTupleDesc().equals(td)){
            throw new DbException("类型不匹配");
        }

        // 查询 tuple
        for (int i = 0; i < numSlots; i++) {
            // 查看未被使用的槽
            if(!isSlotUsed(i)){
                // 标记使用
                markSlotUsed(i, true);
                // 设置路径
                t.setRecordId(new RecordId(pid, i));
                // 放入槽位
                tuples[i] = t;
                return ;
            }
        }

    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	    // not necessary for lab1
        this.dirty = dirty;
        this.tid = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	    // Not necessary for lab1
        if(dirty){
            return tid;
        }
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
        // 找到槽位
        int slot = i / 8;
        // 偏移
        int move = i % 8;
        // 掩码
        byte mask = (byte) (1 << move);
        // 更新槽位
        if(value){
            // 标记已被使用，更新 0 为 1
            header[slot] |= mask;
        }else{
            // 标记为未被使用，更新 1 为 0
            // 除了该位其他位都是 1 的掩码，也就是该位会与 0 运算, 从而置零
            header[slot] &= ~mask;
        }
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

## HeapFile

### 全代码

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
                assert f != null;
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
        // 获取页面序号
        int pageId = page.getId().getPageNumber();
        // 不能超过最大页面数
        if(pageId > numPages()){
            throw new IllegalArgumentException();
        }
        // 创建写入工具
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        // 跳过前面的页面
        f.seek(pageId * BufferPool.getPageSize());
        // 写入数据
        f.write(page.getPageData());
        // 刷盘
        f.close();
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

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        // 找到相应的页
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        return list;
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
                while(whichPage < (heapFile.numPages() - 1)){
                    whichPage++;
                    // 获取下一页
                    iterator = getPageTuple(whichPage);
                    if(iterator.hasNext()){
                        return iterator.hasNext();
                    }
                }
                // 所有元组获取完毕
                return false;
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

## BufferPoll

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
        if(!pageStore.containsKey(pid)){
            // 获取
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            // 是否超过大小
            if(pageStore.size() >= numPages){
                // 淘汰 (后面的 lab 书写)
                eviction();
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
    private void updateBufferPoll(List<Page> pageList, TransactionId tid){
        for (Page page : pageList){
            page.markDirty(true, tid);
            // 如果缓存池已满，执行淘汰策略
            if(pageStore.size() > numPages){
                eviction();
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
     * 淘汰策略
     * 使用 LRU 算法进行淘汰最近最久未使用
     * */
    private void eviction(){
        // 淘汰尾部节点
        LinkedNode node = removeTail();
        // 移除缓存中的记录
        pageStore.remove(node.pageId);
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
    }
}
```

## 测试

HeapPageWriteTest and HeapFileWriteTest, as well as BufferPoolWriteTest.

# Exercise 4

## Insert

### 参数

- private TransactionId tid;
- private OpIterator child;   // 插入的元组 迭代器
- private final int tableId;   // 要插入的表位置

- private boolean inserted;   // 标志位，避免 fetchNext 无限往下取
- private final TupleDesc tupleDesc;   // 返回的 tuple (用于展示插入了多少的 tuples)

### 方法

- Tuple fetchNext()

  ```java
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
      // some code goes here
      // 还未插入
      if(!inserted){
          // 计算插入了多少行
          inserted = true;
          int count = 0;
          while (child.hasNext()){
              Tuple tuple = child.next();
              try{
                  Database.getBufferPool().insertTuple(tid, tableId, tuple);
                  count++;
              }catch (IOException e){
                  e.printStackTrace();
              }
          }
          // 返回插入的次数 所组成的元组
          Tuple tuple = new Tuple(tupleDesc);
          tuple.setField(0, new IntField(count));
          return tuple;
      }
      return null;
  }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * INSERTS TUPLES READ FROM THE CHILD OPERATOR INTO THE TABLEID SPECIFIED IN THE
 * CONSTRUCTOR
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    // 插入的元组 迭代器
    private OpIterator child;
    // 要插入的表位置
    private final int tableId;

    // 标志位，避免 fetchNext 无限往下取
    private boolean inserted;
    // 返回的 tuple (用于展示插入了多少的 tuples)
    private final TupleDesc tupleDesc;


    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc())){
            throw new DbException("插入的类型错误");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"the number of inserted tuple"});
        this.inserted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // 还未插入
        if(!inserted){
            // 计算插入了多少行
            inserted = true;
            int count = 0;
            while (child.hasNext()){
                Tuple tuple = child.next();
                try{
                    Database.getBufferPool().insertTuple(tid, tableId, tuple);
                    count++;
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            // 返回插入的次数 所组成的元组
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(count));
            return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
```

## Delete

### 参数

- private TransactionId tid;
- private OpIterator child; // 要删除的元组 迭代器

- private boolean isDeleted;  // 是否已经删除
- private final TupleDesc tupleDesc;   // 返回删除的行数量

### 方法

- fetchNext()

  ```java
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
      // some code goes here
      // 如果还没有开始删除
      if(!isDeleted){
          isDeleted = true;
          int count = 0;
          while (child.hasNext()){
              Tuple tuple = child.next();
              try{
                  Database.getBufferPool().deleteTuple(tid, tuple);
                  count++;
              }catch (IOException e){
                  e.printStackTrace();
              }
          }
          Tuple res = new Tuple(tupleDesc);
          res.setField(0, new IntField(count));
          return res;
      }
      return null;
  }
  ```

### 全代码

```java
package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    // 要删除的元组 迭代器
    private OpIterator child;

    // 是否已经删除
    private boolean isDeleted;
    // 返回删除的行数量
    private final TupleDesc tupleDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        isDeleted = false;
        tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"the number of deleted tuple"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        isDeleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // 如果还没有开始删除
        if(!isDeleted){
            isDeleted = true;
            int count = 0;
            while (child.hasNext()){
                Tuple tuple = child.next();
                try{
                    Database.getBufferPool().deleteTuple(tid, tuple);
                    count++;
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            Tuple res = new Tuple(tupleDesc);
            res.setField(0, new IntField(count));
            return res;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
```

## 测试

InsertTest and DeleteTest

# Exercise 5

## BufferPool

### 参数

- 双向链表，使用LRU淘汰策略

  ```java
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
  ```

### 方法

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

## 测试

EvictionTest

# Exercise 6

 执行测试

### 实际的查询语句

```sql
SELECT *
FROM some_data_file1,
     some_data_file2
WHERE some_data_file1.field1 = some_data_file2.field1
  AND some_data_file1.id > 1
```

### 创建数据

testLab2_1.dat

```java
1,100,200
2,100,300
3,400,500
4,500,600
5,600,700

```

testLab2_2.dat

```java
1,100,266
2,100,366
2,100,366
3,400,566
4,500,666

```



### 测试代码

```java
package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.*;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;
import java.io.*;
public class TestLab2 {
    public static void main(String[] argv) throws IOException {
        // construct a 3-column table schema
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = new String[]{"field0", "field1", "field2"};

        TupleDesc td = new TupleDesc(types, names);

        // 将文本转换为二进制文件
        HeapFileEncoder.convert(new File("testLab2_1.dat"), new File("some_data_file1.dat"), BufferPool.getPageSize(), 3, types);
        HeapFileEncoder.convert(new File("testLab2_2.dat"), new File("some_data_file2.dat"), BufferPool.getPageSize(), 3, types);

        // 创建文件 新增表
        HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td);
        Database.getCatalog().addTable(table2, "t2");


        // 查询两个表
        TransactionId tid = new TransactionId();

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // 过滤 table1 的值，过滤条件是 > 1
        Filter sf1 = new Filter(
                new Predicate(0,
                Predicate.Op.GREATER_THAN, new IntField(1)), ss1);

        // 确定要双方要连接的字段
        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // 打印结果
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
```

### 输出结果

```java
2 100 300 1 100 266
2 100 300 2 100 366
2 100 300 2 100 366
3 400 500 3 400 566
4 500 600 4 500 666
```



# Exercise 7

### 创建数据

**创建文件 data.txt**

```java
1,10
2,20
3,30
4,40
5,50
5,50

```

**转换为二进制格式**

```java
java -jar dist/simpledb.jar convert data.txt 2 "int,int"
```

**创建一个文件 catalog.txt**

文件意义是让数据库加载 data.txt文件

```java
data (f1 int, f2 int)
```



### 运行数据库

**启动数据库：**

```shell
java -jar dist/simpledb.jar parser catalog.txt
```

```java
Added table : data with schema INT_TYPE(f1),INT_TYPE(f2),
Computing table stats.
Done.
SimpleDB> 
```

**运行查询语句：**

```java
SimpleDB> select d.f1, d.f2 from data d;
Started a new transaction tid = 0
Added scan of table d
Added select list field d.f1
Added select list field d.f2
The query plan is:
  π(d.f1,d.f2),card:0
  |
scan(data d)

d.f1    d.f2
------------------
1 10
2 20
3 30
4 40
5 50
5 50

 6 rows.
Transaction 0 committed.
----------------
0.15 seconds
```



