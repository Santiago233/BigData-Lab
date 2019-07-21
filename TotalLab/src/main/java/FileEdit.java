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

public class FileEdit{
    private static int count = 0;

    public static class FileEditMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            //System.out.println(value.toString());
            String[] val = value.toString().split("\t");
            String name = val[0];
            String list = val[1];
            String newvalue = String.valueOf(count) + "#" + list;
            LPA.name_label.put(name, count);
            count ++;
            context.write(new Text(name), new Text(newvalue));
        }
    }

    public static class FileEditReducer extends Reducer<Text, Text, Text, Text>{

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
        Job job = new Job(conf, "FileEdit");
        //job.getConfiguration().set("path_out", args[1]);
        //job.getConfiguration().set("N", args[2]);

        job.setJarByClass(FileEdit.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(FileEditMapper.class);
        job.setReducerClass(FileEditReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}