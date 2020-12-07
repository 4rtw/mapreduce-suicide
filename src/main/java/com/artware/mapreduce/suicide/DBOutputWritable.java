package com.artware.mapreduce.suicide;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable
{
    private String country;
    private int number;

    public DBOutputWritable(String country, int number)
    {
        this.country = country;
        this.number = number;
    }

    public void readFields(DataInput in) throws IOException {   }

    public void readFields(ResultSet rs) throws SQLException
    {
        country = rs.getString(1);
        number = rs.getInt(2);
    }

    public void write(DataOutput out) throws IOException {    }

    public void write(PreparedStatement ps) throws SQLException
    {
        ps.setString(1, country);
        ps.setInt(2, number);
    }
}