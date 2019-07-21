import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.List;
import java.io.*;
import java.util.*;

public class MenPairs{

    public static class MenPairsMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected  void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
            ArrayList<String> NameList = new ArrayList<String>();
            String[] names = value.toString().split("[\\n\\t\\s]+");
            for(String tmp1 : names){
                if(NameList.contains(tmp1) == false){
                    NameList.add(tmp1);
                }
            }
            for(String tmp2 : NameList){
                for(String tmp3 : NameList){
                    if(tmp3.equals(tmp2) == false){
                        Text pair = new Text();
                        pair.set(tmp3 + "#" + "1");
                        context.write(new Text(tmp2), pair);
                    }
                }
            }
        }
    }

    /*public static class NewPartitioner extends HashPartitioner<Text, Text>{

        public int getPartitioner(Text key, LongWritable value, int numReduceTasks){
            String term = new String();
            term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }*/

    public static class MenPairsReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
            StringBuilder out = new StringBuilder();
            ArrayList<String> Name = new ArrayList<String>();

            /*for(Text value : values){
                System.out.println(key.toString() + value.toString() + "\r");
            }
            System.out.println("\n");*/

            List<Text> list = new ArrayList<Text>();    //Double-Iterator does not work!
            for(Text value : values){
                list.add(new Text(value));
            }
            //Iterator<Text> it1 = values.iterator();
            //Iterator<Text> it3 = values.iterator();
            for(Text a : list){
                String tmp1 = new String();
                tmp1 = a.toString().split("#")[0];
                /*if(key.toString().equals("上官")) {
                    System.out.println(tmp1);
                }*/
                if(Name == null || Name.contains(tmp1) == false) {
                    int count = 0;
                    //Iterator<Text> it2 = it3;
                    for (Text b : list) {
                        String tmp2 = new String();
                        tmp2 = b.toString().split("#")[0];
                        /*if(key.toString().equals("上官")) {
                            System.out.println(tmp2);
                        }*/
                        if (tmp2.equals(tmp1)) {
                            String number = new String();
                            number = b.toString().split("#")[1];
                            count += Integer.parseInt(number);
                        }
                    }
                    Name.add(tmp1);
                    out.append("<" + key.toString() + "," + tmp1 + ">" + " " + Integer.valueOf(count));
                }
            }
            String str1 = out.toString();
            //System.out.println(str1);
            //String[] str2 = str1.split("\n");
        /*    String path_name = context.getConfiguration().get("path_out");
            File file = new File(path_name + "Task2");
            //PrintStream ps = new PrintStream(new FileOutputStream(file));
            //ps.println(out.toString());
            FileWriter fw = null;
            fw = new FileWriter(file, true);
            PrintWriter pw = new PrintWriter(fw);
            pw.println(str1);
            pw.flush();
            fw.flush();
            pw.close();
            fw.close(); //continue writing!*/
            context.write(null, new Text(str1));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MenPairs");

        System.out.println("File Read Path:");
        Scanner sc1 = new Scanner(System.in);
        String path_in = sc1.nextLine();
        System.out.println("File Written Path:");
        Scanner sc2 = new Scanner(System.in);
        String path_out = sc2.nextLine();
        job.getConfiguration().set("path_out", path_out);
        Path in = new Path(path_in);
        Path out = new Path(path_out);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJarByClass(MenPairs.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MenPairsMapper.class);
        //job.setPartitionerClass(HashPartitioner.class);
        job.setReducerClass(MenPairsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}