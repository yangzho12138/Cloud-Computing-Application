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

public class TablePartE{

   public static void main(String[] args) throws IOException {

	Configuration config = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(config);

	Table table = connection.getTable(TableName.valueOf("powers"));

	Scan scan = new Scan();
	scan.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));
	scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
	scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));
	scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("xp"));


	ResultScanner scanner = table.getScanner(scan);
	for (Result result = scanner.next(); result != null; result = scanner.next())
		System.out.println(result);
   }
}

