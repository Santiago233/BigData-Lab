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
import java.util.Scanner;
import java.io.*;
import java.text.DecimalFormat;

public class MenMap{

    public static class MenMapMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException{
            //System.out.println(value.toString());
            //System.out.println("end");
            //String text1 = value.toString().replaceAll("[&nbsp]", "");  //replace space paragraph
            //String text2 = text1.replaceAll("<", "");
            String text1 = value.toString().replaceAll("><", "#");
            String text2 = text1.replaceAll("<", "");
            String text3 = text2.replaceAll(">", "");
            String text4 = text3.replaceAll(" ", ",");
            //System.out.println(text4);
            String[] NameList = text4.split("#");
            int count = 0;
            String name = "";
            for(String val : NameList){
                String[] Label = val.split(",");
                count += Integer.parseInt(Label[2]);
                if(name == ""){
                    name = Label[0];
                }
            }
            //System.out.println(count);

            StringBuilder out = new StringBuilder();
            for(String val : NameList){
                String[] Label = val.split(",");
                //Text newkey = new Text(Label[0]);
                //Text newvalue = new Text(Label[1] + "#" + Label[2]);
                //System.out.println(newkey.toString() + " " + newvalue.toString());
                //context.write(newkey, newvalue);
                String object = Label[1];
                String number = Label[2];
                DecimalFormat df = new DecimalFormat("###.00000");
                String number_d = "0" + df.format(Integer.parseInt(number) * 1.0 / count);
                //System.out.println(number_d);
                if(number_d.indexOf(".") > 0){
                    number_d = number_d.replaceAll("0+?$", "");
                }
                if(object.equals(NameList[NameList.length - 1].split(",")[1]) == false) {
                    out.append(object + "," + number_d + "|");
                }else{
                    out.append(object + "," + number_d);
                }
            }
            String str_out = name + " [" + out.toString() + "]";
            //System.out.println(str_out);
            //context.write(new Text(name), new Text(str_out));
            String path_name = context.getConfiguration().get("path_out");
            File file = new File(path_name + "Task3");
            //PrintStream ps = new PrintStream(new FileOutputStream(file));
            //ps.println(out.toString());
            FileWriter fw = null;
            fw = new FileWriter(file, true);
            PrintWriter pw = new PrintWriter(fw);
            pw.println(str_out);
            pw.flush();
            fw.flush();
            pw.close();
            fw.close(); //continue writing!
        }
    }

    public static class MenMapReducer extends Reducer<Text, Text, Text, Text>{

        protected void reducer(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "MenMap");

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

        job.setJarByClass(MenMap.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MenMapMapper.class);
        job.setReducerClass(MenMapReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}