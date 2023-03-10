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


public class TablePartA{

   public static void main(String[] args) throws IOException {
      Configuration config = HBaseConfiguration.create();
      config.set("hbase.zookeeper.quorum", "localhost");
      config.set("hbase.zookeeper.property.clientPort", "2181");

      HBaseAdmin admin = new HBaseAdmin(config);

      HTableDescriptor tableDescriptor1 = new HTableDescriptor(TableName.valueOf("powers"));
      tableDescriptor1.addFamily(new HColumnDescriptor("personal"));
      tableDescriptor1.addFamily(new HColumnDescriptor("professional"));
      tableDescriptor1.addFamily(new HColumnDescriptor("custom"));

      admin.createTable(tableDescriptor1);

      HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("food"));
      tableDescriptor2.addFamily(new HColumnDescriptor("nutrition"));
      tableDescriptor2.addFamily(new HColumnDescriptor("taste"));

      admin.createTable(tableDescriptor2);

      admin.close();
   }
}