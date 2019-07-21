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

public class FileOutput{

    public static class FileOutputMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
            String label_list = value.toString();
            String name = label_list.split("\t")[0];
            String label = label_list.split("\t")[1].split("#")[0];
            int label_num = Integer.parseInt(label);
            String label_name = null;
            for(String i : LPA.name_label.keySet()){
                if(LPA.name_label.get(i) == label_num){
                    label_name = i;
                }
            }
            context.write(new Text(name), new Text(label_name));
        }
    }

    public static class FileOutputReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            for(Text value : values){
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "FileOutput");

        job.setJarByClass(FileOutput.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(FileOutputMapper.class);
        job.setReducerClass(FileOutputReducer.class);
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