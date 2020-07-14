package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * 1.新增和修改数据
 * 2.单条数据查询
 * 3.批量数据查询
 * 4.删除数据
 */
public class HBase02_DML {

    //声明Connection以及Admin
    private static Connection connection;

    static {
        //1.创建配置信息,并指定连接的集群
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //2.创建连接器
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException {
        connection.close();
    }
    //TODO 新增和修改数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {

        //1.获取DML的Table对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //3.给Put对象添加数据
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("female"));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("age"), Bytes.toBytes("18"));

        //4.执行插入数据的操作
        table.put(put);

        //5.释放资源
        table.close();
    }

    //TODO 单条查询数据
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        //指定列族
        get.addFamily(Bytes.toBytes(cf));
        //指定列族:列
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //3.查询数据
        Result result = table.get(get);

        //4.解析result
        for (Cell cell : result.rawCells()) {
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //5.释放资源
        table.close();
    }

    //TODO 扫描数据Scan
    public static void scanTable(String tableName) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建Scan对象
        Scan scan = new Scan();

        scan.withStartRow(Bytes.toBytes("1000"));
        scan.withStopRow(Bytes.toBytes("1004"), true);

        //3.扫描全表
        ResultScanner results = table.getScanner(scan);

        //4.解析results
        Iterator<Result> iterator = results.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            for (Cell cell : result.rawCells()) {
                System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        //5.释放资源
        table.close();
    }

    //TODO 删除数据
    //DeleteFamily:执行删除整个RowKey数据所添加的标记，作用范围:当前列族小于等于标记时间戳的数据
    //DeleteColumn：执行指定到列族的列数据所添加的标记(带"s"的方法)，作用范围：当前列小于等于标记时间戳的数据
    //Delete:执行指定到列族的列数据所添加的标记,作用范围：只作用于标记中所携带的时间戳的范围
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //2.创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //指定列族删除数据
//        delete.addFamily(Bytes.toBytes(cf));

        //指定列族和列
//        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));

        //3.执行删除操作
        table.delete(delete);

        //4.释放资源
        table.close();

    }

    public static void main(String[] args) throws IOException {

        //测试插入数据
//        putData("stu8", "1004", "info2", "name", "XCC");

        //测试Get方式获取数据
//        getData("stu", "1004", "info2", "age");

        //测试扫描全表
        scanTable("stu");

        //测试删除数据
//        deleteData("student", "1008", "info1", "name");
          close();
    }


}
