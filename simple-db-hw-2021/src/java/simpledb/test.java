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
