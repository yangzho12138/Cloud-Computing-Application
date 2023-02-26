import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class TablePartD{

   public static void main(String[] args) throws IOException {
	Configuration config = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(config);

	Table table = connection.getTable(TableName.valueOf("powers"));

	Get get = new Get(Bytes.toBytes("row1"));
	Result result = table.get(get);
	
	String hero = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("hero")));
	String power = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power")));
	String name = Bytes.toString(result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
	String xp = Bytes.toString(result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("xp")));
	String color = Bytes.toString(result.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));
	System.out.println("hero: "+hero+", power: "+power+", name: "+name+", xp: "+xp+", color: "+color);


	get = new Get(Bytes.toBytes("row19"));
	result = table.get(get);
	hero = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("hero")));
	color = Bytes.toString(result.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));
	System.out.println("hero: "+hero+", color: "+color);

	get = new Get(Bytes.toBytes("row1"));
	result = table.get(get);
	hero = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("hero")));
	name = Bytes.toString(result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
	color = Bytes.toString(result.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));
	System.out.println("hero: "+hero+", name: "+name+", color: "+color); 
   }
}

