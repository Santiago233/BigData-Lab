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
import java.util.ArrayList;
import java.util.*;
import java.io.*;
import java.text.DecimalFormat;

public class LabelProcess{

    public static class LabelProcessMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
            String name = value.toString().split("\t")[0];
            String label = value.toString().split("\t")[1];
            context.write(new Text(label), new Text(name));
        }
    }

    public static class LabelProcessReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            List<Text> list = new ArrayList<Text>();
            for(Text value: values){
                list.add(new Text(value));
            }
            StringBuilder label_list = new StringBuilder();
            for(Text a : list){
                label_list.append(a.toString());
                if(a.toString().equals(list.get(list.size()-1)) == false){
                    label_list.append(",");
                }
            }
            context.write(key, new Text(label_list.toString()));
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "LabelProcess");

        /*System.out.println("File Read Path:");
        Scanner sc1 = new Scanner(System.in);
        String path_in = sc1.nextLine();
        System.out.println("File Written Path:");
        Scanner sc2 = new Scanner(System.in);
        String path_out = sc2.nextLine();
        Path in = new Path(path_in);
        Path out = new Path(path_out);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);*/
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(LabelProcess.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LabelProcessMapper.class);
        job.setReducerClass(LabelProcessReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}