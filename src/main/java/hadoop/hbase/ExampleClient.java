package hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/****
 * 
 * 执行方法: hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.hbase.ExampleClient
 * 
 * @author jay
 *
 */

public class ExampleClient
{

	public static void main(String[] ar) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
	{
		Configuration conf = HBaseConfiguration.create();

		@SuppressWarnings("deprecation")
		HBaseAdmin admin = new HBaseAdmin(conf);
		@SuppressWarnings("deprecation")
		HTableDescriptor htd = new HTableDescriptor("test");

		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);

		byte[] tablename = htd.getName();
		HTableDescriptor[] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName()))
		{
			throw new IOException("Failed of Creating Table");
		}

		HTable table = new HTable(conf, tablename);
		byte[] row1 = Bytes.toBytes("row1");

		Put p1 = new Put(row1);
		byte[] databytes = Bytes.toBytes("data");
		p1.add(databytes, Bytes.toBytes("1"), Bytes.toBytes("value1"));
		table.put(p1);

		Get g1 = new Get(row1);
		Result result = table.get(g1);
		System.out.println("ExampleClient-------------->>>>Get: " + result);

		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);

		for (Result s : scanner)
		{
			System.out.println("ExampleClient-------------->>>>Scan: " + s);
		}

		scanner.close();

		admin.disableTable(tablename);
		admin.deleteTable(tablename);

	}

}
