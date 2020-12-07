package com.artware.mapreduce.suicide;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBInputWritable implements Writable, DBWritable {

    private String country;
    private int number;

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, country);
        preparedStatement.setInt(2, number);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        country = resultSet.getString(1);
        number = resultSet.getInt(2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeInt(number);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country = dataInput.readUTF();
        number = dataInput.readInt();
    }

    public int getNumber()
    {
        return number;
    }

    public String getCountry()
    {
        return country;
    }
}
