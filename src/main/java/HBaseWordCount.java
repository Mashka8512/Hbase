import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hsqldb.Table;

import static java.nio.charset.StandardCharsets.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public class HBaseWordCount extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job GetJobConf(Configuration conf, String[] args) throws IOException {
        String input_table_1 = args[0];
        String input_table_2 = args[1];

        conf.set("pages_table", input_table_1);
        conf.set("sites_table", input_table_2);

        Job job = Job.getInstance(conf);
        job.setJarByClass(HBaseWordCount.class);
        job.setJobName(HBaseWordCount.class.getCanonicalName());

        List<Scan> scans = new ArrayList<Scan>();

        Scan scan1 = new Scan();
        scan1.setAttribute("scan.attributes.table.name", Bytes.toBytes(input_table_1));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.setAttribute("scan.attributes.table.name", Bytes.toBytes(input_table_2));
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(
                scans,
                DemoMapper.class,
                Text.class,
                Text.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                input_table_1,
                DemoReducer.class,
                job);

        job.setReducerClass(DemoReducer.class);


        return job;
    }

    static public class DemoMapper extends TableMapper<Text, Text> {
        private static byte[] pages_table;
        private static byte[] sites_table;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            pages_table = Bytes.toBytes(conf.get("pages_table"));
            sites_table = Bytes.toBytes(conf.get("sites_table"));
        }


        private static String GetDomain(String real_url){
            String url = real_url;
            url = url.replace("https://", "");
            url = url.replace("http://", "");
            url = url.replace(" ", "");
            url = url.replace("://", "");
            return url.split("/")[0];
        }


        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            TableSplit currentSplit = (TableSplit)context.getInputSplit();
            byte[] tableName = currentSplit.getTableName();
            try {
                if (Arrays.equals(tableName, pages_table)) {
                    Cell cell = value.getColumnLatestCell(Bytes.toBytes("docs"), Bytes.toBytes("url"));
                    String url = new String(CellUtil.cloneValue(cell), UTF_8);
                    if(value.containsColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"))){
                        Cell cell_dis = value.getColumnLatestCell(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                        String disabled = new String(CellUtil.cloneValue(cell_dis), UTF_8);
                        context.write(new Text(GetDomain(url)), new Text(url + "\t" + disabled));
                    }else{
                        context.write(new Text(GetDomain(url)), new Text(url));
                    }

                } else if (Arrays.equals(tableName, sites_table)) {
                    Cell cell = value.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("site"));
                    String site = new String(CellUtil.cloneValue(cell), UTF_8);
                    if(value.containsColumn(Bytes.toBytes("info"), Bytes.toBytes("robots"))){
                        Cell cell_robots = value.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("robots"));
                        String robots = new String(CellUtil.cloneValue(cell_robots), UTF_8);
                        context.write(new Text(site), new Text(robots));
                    }

                }
            } catch (Exception e) {
               System.out.println("Error: " + e.toString());
            }

        }
    }

    static public class DemoReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

        private static String GetDomain(String real_url){
            String url = real_url;
            url = url.replace("https://", "");
            url = url.replace("http://", "");
            url = url.replace(" ", "");
            url = url.replace("://", "");
            return url.split("/")[0];
        }


        static private void process_robots(String robots, ArrayList stop_1, ArrayList stop_2, ArrayList stop_3){
            robots = robots.replaceAll("Disallow: ", "");
            String[] parts = robots.split("\n");
         /* Disallow: /info (запрещает пути, начинающиеся с /info: /info, /info/help.html, ...)
            Disallow: *forum (запрещает все пути, где есть слово forum: /forum, /attic/forum27/, ...)
            Disallow: remove.php$ (запрещает все пути, оканчивающиеся на remove.php)*/
            for(String part : parts){
                if(part.startsWith("*")){
                    part = part.substring(1);
                    if(part.startsWith("/")){
                        if(part.endsWith("$")){
                            part = part.substring(0, part.length() - 1);
                            stop_3.add(part);
                        }else{
                            stop_2.add(part);
                        }
                    }else{
                        if(part.endsWith("$")){
                            part = part.substring(0, part.length() - 1);
                            stop_3.add(part);
                        }else{
                            stop_2.add(part);
                        }
                    }
                }else{
                    if(part.startsWith("/")){
                        if(part.endsWith("$")){
                            stop_3.add(part.substring(0, part.length() - 1));
                        }else{
                            stop_1.add(part);
                        }
                    }else{
                        if(part.endsWith("$")){
                            stop_3.add(part.substring(0, part.length() - 1));
                        }
                    }

                }

            }
        }

        static private Boolean check(String url, ArrayList<String> stop_1, ArrayList<String> stop_2, ArrayList<String> stop_3){
            String domain = GetDomain(url);
            String new_url = url.replaceFirst(domain, "");
            new_url = new_url.replace("http://", "").replace("https://", "");
            for(String word : stop_1){
                if(new_url.startsWith(word)){
                    return Boolean.FALSE;
                }
            }

            for(String word : stop_2){
                if(new_url.contains(word)){
                    return Boolean.FALSE;
                }
            }

            for(String word : stop_3){
                if(new_url.endsWith(word)){
                    return Boolean.FALSE;
                }
            }


            return Boolean.TRUE;


        }

        static private String get_hash(String url)  {
            return DigestUtils.md5Hex(url);
        }



        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String value;
            ArrayList<String> urls = new ArrayList<>();
            String robots = "";
            Boolean activate;
            Boolean check;
            String[] parts;
            String real_url;
            for(Text value_ : values){
                value = value_.toString();
                // It's robots.txt
                if(value.contains("Disallow: ")){
                    robots = value;
                }else{
                    urls.add(value);
                }
            }

            if(robots.equals("")){
                for(String url : urls){
                    parts = url.split("\t");
                    if(parts.length > 1){
                        real_url = parts[0];
                        String hash = get_hash(real_url);
                        Delete delete = new Delete(Bytes.toBytes(hash));
                        delete.addColumns(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                        context.write(new ImmutableBytesWritable(hash.getBytes()), delete);
                    }
                }

            }else{
                ArrayList<String> stop_1 = new ArrayList<>();
                ArrayList<String> stop_2 = new ArrayList<>();
                ArrayList<String> stop_3 = new ArrayList<>();

                process_robots(robots, stop_1, stop_2, stop_3);
                for(String url : urls){
                    activate = Boolean.TRUE;
                    parts = url.split("\t");
                    real_url = parts[0];
                    if(parts.length > 1){
                        activate = Boolean.FALSE;
                    }
                    check = check(real_url, stop_1, stop_2, stop_3);

                    if((check & activate) || (!check & !activate)){
                        // nothing to do
                        continue;
                    }
                    if(check & !activate){
                        // we need to delete column disabled
                        System.out.println("Delete");
                        String hash = get_hash(real_url);
                        Delete delete = new Delete(Bytes.toBytes(hash));
                        delete.addColumns(Bytes.toBytes("docs"), Bytes.toBytes("disabled"));
                        context.write(new ImmutableBytesWritable(hash.getBytes()), delete);
                    }
                    if(!check & activate){
                        // we need to add column disabled
                        System.out.println("Add");
                        String hash = get_hash(real_url);
                        Put put = new Put(hash.getBytes());
                        put.addColumn(Bytes.toBytes("docs"), Bytes.toBytes("disabled"), Bytes.toBytes("Y"));
                        context.write(new ImmutableBytesWritable(hash.getBytes()), put);
                    }

                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(HBaseConfiguration.create(), new HBaseWordCount(), args);
        System.exit(rc);
    }
}
