import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;
import java.text.DecimalFormat;

public class PageRankIter{

    public static class PageRankIterMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            //System.out.println(value.toString());
            String[] url_list = value.toString().split("\t");
            String URL = url_list[0];
            String pr_init = url_list[1].split("#")[0];
            double pr_init_d = Double.parseDouble(pr_init);
            //System.out.println(pr_init_d);
            String link_list_init = url_list[1].split("#")[1];
            String link_list = link_list_init.replaceAll("\\[", "").replaceAll("]", "");
            //System.out.println(link_list);
            String[] links = link_list.split("\\|");    //Ugly Escape Character!!!
            for(String x : links){
                //System.out.println(x);
                String uname = x.split(",")[0];
                String weight = x.split(",")[1];
                double weight_d = Double.parseDouble(weight);
                DecimalFormat df = new DecimalFormat("###.00000");
                String pr_u = "0" + df.format(pr_init_d * weight_d);
                /*if(URL.equals("一灯大师") == true) {
                    System.out.println(uname + " " + pr_u + "\r");
                }*/
                context.write(new Text(uname), new Text(pr_u));
            }
            context.write(new Text(URL), new Text(link_list_init));
        }
    }

    public static class PageRankIterReducer extends Reducer<Text, Text, Text, Text>{

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            //System.out.println("reduce here!");
            double weight = 0;
            String rank_list = null;
            for(Text value : values){
                String val = value.toString();
                //System.out.println(val);
                if(val.startsWith("0")){
                    weight += Double.parseDouble(val);
                }else{
                    //System.out.println("here");
                    rank_list = val;
                }
            }
            //System.out.println(rank_list);
            DecimalFormat df = new DecimalFormat("###.00000");
            String weight_str = "0" + df.format(weight);
            //System.out.println(number_d);
            if(weight_str.indexOf(".") > 0){
                weight_str = weight_str.replaceAll("0+?$", "");
            }
            String newvalue = weight_str + "#" + rank_list;
            context.write(new Text(key), new Text(newvalue));
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "PageRankIter");

        job.setJarByClass(PageRankIter.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(PageRankIterMapper.class);
        job.setReducerClass(PageRankIterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //System.out.println(args[0] + " " + args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}