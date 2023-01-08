package com.ysear.plugin.mysql.datasource;

import com.ysear.plugin.datasource.jdbc.JDBCBase;

/*
 * @Author Administrator
 * @Date 2023/1/6
 **/
public class MysqlDataSource extends JDBCBase {

    public MysqlDataSource(String type, String id, String driverClass, String url, String userName, String passWord) {
        super(type, id,driverClass,url,userName, passWord);
    }

    @Override
    public String toString() {
        return "MysqlDataSourceParam{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", driverClass='" + driverClass + '\'' +
                ", url='" + url + '\'' +
                ", userName='" + userName + '\'' +
                ", passWord='" + passWord + '\'' +
                '}';
    }
}
