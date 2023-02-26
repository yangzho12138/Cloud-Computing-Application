import java.io.IOException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;

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

public class TablePartC{

   public static void main(String[] args) throws IOException {
      Configuration config = HBaseConfiguration.create();
      config.set("hbase.zookeeper.quorum", "localhost");
      config.set("hbase.zookeeper.property.clientPort", "2181");

      Connection connection = ConnectionFactory.createConnection(config);

      HBaseAdmin admin = new HBaseAdmin(config);

      // read data from csv
      BufferedReader reader = new BufferedReader(new FileReader("input.csv"));
      Table table = connection.getTable(TableName.valueOf("powers"));
      String line;
      while ((line = reader.readLine()) != null) {
         String[] fields = line.split(",");
         String rowKey = fields[0];
         Put put = new Put(Bytes.toBytes(rowKey));
         put.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes(fields[1]));
         put.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes(fields[2]));
         put.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes(fields[3]));
         put.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes(fields[4]));
         put.add(Bytes.toBytes("custom"), Bytes.toBytes("color"), Bytes.toBytes(fields[5]));
         table.put(put);
      }

      reader.close();
      table.close();
      admin.close();
   }
}

