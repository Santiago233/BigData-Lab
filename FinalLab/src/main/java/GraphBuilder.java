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

public class GraphBuilder{

    public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text>{

        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            //System.out.println(value.toString());
            String[] url = value.toString().split("\t");
            /*if(url[0].equals("一灯大师")){
                System.out.println(url[1]);
            }*/
            //String url_number = context.getConfiguration().get("url_number");
            //System.out.println(url_number);
            //String url_number = MainLab.N.toString();
            DecimalFormat df = new DecimalFormat("###.00000");
            String PR_init = "0" + df.format(1.0 / MainLab.N);
            if(PR_init.indexOf(".") > 0){
                PR_init= PR_init.replaceAll("0+?$", "");
            }
            //String PR_init = "1";
            /*if(url[0].equals("一灯大师")){
                System.out.println(PR_init);
            }*/
            String newkey = url[0];
            String newvalue = PR_init + "#" + url[1];
            context.write(new Text(newkey), new Text(newvalue));
            //System.out.println(newkey + " " + newvalue);
            /*String str_out = newkey + " " + newvalue;
            String path_name = context.getConfiguration().get("path_out");
            File file = new File(path_name + "/Task4");
            FileWriter fw = null;
            fw = new FileWriter(file, true);
            PrintWriter pw = new PrintWriter(fw);
            pw.println(str_out);
            pw.flush();
            fw.flush();
            pw.close();
            fw.close();*/
        }
    }

    public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text>{

        protected void reduce(Text key, Iterable<Text>values, Context context)
                throws IOException, InterruptedException{
            //System.out.println(values.toString());
            for(Text value : values){
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "GraphBuilder");
        job.getConfiguration().set("path_out", args[1]);
        job.getConfiguration().set("url_number", args[2]);

        job.setJarByClass(GraphBuilder.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(GraphBuilderMapper.class);
        job.setReducerClass(GraphBuilderReducer.class);
        //job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}