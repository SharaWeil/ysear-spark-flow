package com.ysear.plugin.datasource.dialect;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import scala.Option;

/**
 * @author ysera
 * @date 2022/12/26
 */
public class OracleJdbcDialect extends JdbcDialect {
    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle");
    }

    @Override
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
        if (sqlType == 2) {
            long scale = (null != md) ? md.build().getLong("scale") : 0L;
            return (0L == scale) ? Option.apply(DataTypes.IntegerType) : Option.apply(DataTypes.DoubleType);
        }
        if (sqlType == 12) {
            return Option.apply(DataTypes.StringType);
        }
        if (sqlType == 91 && typeName.equals("DATE") && size == 0) {
            return Option.apply(DataTypes.TimestampType);
        }
        return Option.empty();
    }

    @Override
    public Option<JdbcType> getJDBCType(DataType dt) {
        if (DataTypes.StringType.sameType(dt)) {
            return Option.apply(new JdbcType("VARCHAR2(255)", 12));
        }
        if (DataTypes.BooleanType.sameType(dt)) {
            return Option.apply(new JdbcType("NUMBER(1)", 2));
        }
        if (DataTypes.IntegerType.sameType(dt)) {
            return Option.apply(new JdbcType("NUMBER(10)", 2));
        }
        if (DataTypes.LongType.sameType(dt)) {
            return Option.apply(new JdbcType("NUMBER(19)", 2));
        }
        if (DataTypes.DoubleType.sameType(dt)) {
            return Option.apply(new JdbcType("NUMBER(19,4)", 2));
        }
        if (DataTypes.FloatType.sameType(dt)) {
            return Option.apply(new JdbcType("NUMBER(19,2)", 2));
        }
        if (DataTypes.ShortType.sameType(dt)) {
            return Option.apply(new JdbcType("NUMBER(5)", 2));
        }
        if (DataTypes.ByteType.sameType(dt)) {
            return Option.apply(new JdbcType("NUMBER(3)", 2));
        }
        if (DataTypes.BinaryType.sameType(dt)) {
            return Option.apply(new JdbcType("BLOB", 2004));
        }
        if (DataTypes.TimestampType.sameType(dt)) {
            return Option.apply(new JdbcType("DATE", 91));
        }
        if (DataTypes.DateType.sameType(dt)) {
            return Option.apply(new JdbcType("DATE", 91));
        }
        if (DataTypes.createDecimalType().sameType(dt)) {
            return Option.apply(new JdbcType("NUMBER(38,4)", 2));
        }
        return Option.apply(new JdbcType("VARCHAR2(255)", 12));
    }
}
