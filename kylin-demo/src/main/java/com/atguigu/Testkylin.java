package com.atguigu;

import java.sql.*;

public class Testkylin {
    public static void main(String[] args) throws Exception {

        //Kylin_JDBC 驱动
        String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";

        //Kylin_URL
        String KYLIN_URL = "jdbc:kylin://hadoop102:7070/firstProject";

        //Kylin的用户名
        String KYLIN_USER = "ADMIN";

        ///Kylin的密码
        String KYLIN_PASSWORD = "KYLIN";

        //添加驱动信息
        Class.forName(KYLIN_DRIVER);

        //获取连接
        Connection connection = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWORD);

        //预编译SQL
        PreparedStatement prepareStatement = connection.prepareStatement("SELECT PROVINCE_ID,count (*)" +
                "sum FROM DWD_FACT_ORDER_DETAIL group BY PROVINCE_ID");

        //执行查询
        ResultSet resultSet = prepareStatement.executeQuery();

        //遍历打印
        while (resultSet.next()){
            System.out.println("PROVINCE_ID:" + resultSet.getInt(1)
                             + "CT:" + resultSet.getLong(2));
        }

        System.out.println("hotFix");

        //关闭资源
        resultSet.close();
        prepareStatement.close();
        connection.close();
    }
}
