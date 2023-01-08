package com.ysear.plugin.datasource.jdbc;

import com.ysear.spark.spi.datasource.BaseDataSource;

/*
 * @Author Administrator
 * @Date 2023/1/8
 **/
public class JDBCBase extends BaseDataSource {
    protected String id;
    protected String type;
    protected String driverClass;
    protected String url;
    protected String userName;
    protected String passWord;

    public JDBCBase(String type, String id, String driverClass, String url, String userName, String passWord) {
        super(type, id);
        this.id = id;
        this.type = type;
        this.driverClass = driverClass;
        this.url = url;
        this.userName = userName;
        this.passWord = passWord;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassWord() {
        return passWord;
    }

    public void setPassWord(String passWord) {
        this.passWord = passWord;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    @Override
    public String toString() {
        return "JDBCBaseParam{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", driverClass='" + driverClass + '\'' +
                ", url='" + url + '\'' +
                ", userName='" + userName + '\'' +
                ", passWord='" + passWord + '\'' +
                '}';
    }
}
