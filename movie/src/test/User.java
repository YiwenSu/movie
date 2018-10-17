package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class User implements Writable, DBWritable {

	private int id;  
    private String name;  
  
    // 无参构造函数  
    public User() {  
    }  
  
    public int getId() {  
        return id;  
    }  
  
    public void setId(int id) {  
        this.id = id;  
    }  
  
    public String getName() {  
        return name;  
    }  
  
    public void setName(String name) {  
        this.name = name;  
    }  
  
    // 实现DBWritable接口要实现的方法  
    public void readFields(ResultSet resultSet) throws SQLException {  
        this.id = resultSet.getInt(1);  
        this.name = resultSet.getString(2);  
    }  
  
    // 实现DBWritable接口要实现的接口  
    public void write(PreparedStatement preparedStatement) throws SQLException {  
        preparedStatement.setInt(1, this.id);  
        preparedStatement.setString(2, this.name);  
    }  
  
    // 实现Writable接口要实现的方法  
    public void readFields(DataInput dataInput) throws IOException {  
        this.id = dataInput.readInt();  
        this.name = Text.readString(dataInput);  
    }  
  
    // 实现Writable接口要实现的接口  
    public void write(DataOutput dataOutput) throws IOException {  
        dataOutput.writeInt(this.id);  
        Text.writeString(dataOutput, this.name);  
    }  
  
    @Override  
    public String toString() {  
        return "User [id=" + id + ", name=" + name + "]";  
    }  
}
