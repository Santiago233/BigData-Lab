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

public class LabelSet{

    public static class LabelSetMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
            //System.out.println(value.toString());
            String len = context.getConfiguration().get("len");
            int N = Integer.parseInt(len);
            int[] array = new int[N];
            for(int i : array){i = 0;}
            String name = value.toString().split("\t")[0];
            String label_list = value.toString().split("\t")[1];
            String[] unames = label_list.split("#")[1].replace("[", "").replace("]", "").split("\\|");  //split needs Escape, but replace not
            //System.out.println(unames);
            for(int i = 0; i < unames.length; i++){
                //System.out.println(unames[i]);
                int order = LPA.name_label.get(unames[i].split(",")[0]);
                //System.out.println(order);
                array[order] ++;
            }
            //for(int i = 0; i < array.length; i++){System.out.println(array[i]);}
            int sum = 0;
            int label = 0;
            for(int i = 0; i < array.length; i++){
                if(array[i] >= sum){
                    sum = array[i];
                    label = i;
                }
            }
            String newvalue = String.valueOf(label) + "#" + label_list.split("#")[1];
            context.write(new Text(name), new Text(newvalue));
        }
    }

    public static class LabelSetReducer extends Reducer<Text, Text, Text, Text>{

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
        Job job = new Job(conf, "LabelSet");
        job.getConfiguration().set("len", args[2]);

        job.setJarByClass(LabelSet.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LabelSetMapper.class);
        job.setReducerClass(LabelSetReducer.class);
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