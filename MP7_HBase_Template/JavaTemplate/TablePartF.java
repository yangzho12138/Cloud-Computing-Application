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

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.util.*;

public class TablePartF{

   public static void main(String[] args) throws IOException {

	// TODO      
	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER 
	Configuration config = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(config);

	Table table = connection.getTable(TableName.valueOf("powers"));

	Scan scan = new Scan();
	scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
	scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));
	scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("scan"));
	scan.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));


	ResultScanner scanner = table.getScanner(scan);

	List<Result> results = new ArrayList<>();
	for(Result result : scanner){
		results.add(result);
	}

	for (Result result : results){
		for (Result result1 : results){
			String name = Bytes.toString(result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
			String power = Bytes.toString(result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power")));
			String color = Bytes.toString(result.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));

			String name1 = Bytes.toString(result1.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name")));
			String power1 = Bytes.toString(result1.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power")));
			String color1 = Bytes.toString(result1.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color")));
			
			if(color.equals(color1) && !name.equals(name1)){
				System.out.println(name + ", " + power + ", " + name1 + ", " + power1 + ", "+color);
			}
		}
   	}

   }
}
