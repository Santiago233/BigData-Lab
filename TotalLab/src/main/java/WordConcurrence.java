import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.ansj.domain.Term;
import org.ansj.domain.Result;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.io.IOException;
import java.util.Scanner;
import java.util.List;
import java.io.*;
import java.util.*;

public class WordConcurrence{

    public static class WordConcurrenceMapper extends Mapper<Object, Text, Text, Text>{

        private ArrayList<String> NameList = new ArrayList<String>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException{
            //System.out.println("Here:setup\r");
            /*String path_name = context.getConfiguration().get("path_name");

            Configuration conf = new Configuration();   //access to the content of file to people's name
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path(path_name);
            FSDataInputStream inStream = fs.open(file);
            String data = inStream.readUTF();
            inStream.close();
            //System.out.println(data);
            BufferedReader reader = new BufferedReader(new StringReader(data));*/
            String namepath = WordConcurrenceMapper.class.getClassLoader().getResource("People_List_unique.txt").getPath();
            BufferedReader reader = new BufferedReader(new FileReader(new File(namepath)));
            String PersonName;
            while((PersonName = reader.readLine()) != null){
                //System.out.println(PersonName);
                NameList.add(PersonName);
                DicLibrary.insert(DicLibrary.DEFAULT, PersonName);
            }
            //System.out.println("Library Finished\r");
            /*for (String tmp : NameList) {
                System.out.println(tmp);
            }*/
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            //System.out.println("Here:map\r");
            String paragraph = value.toString();
            //System.out.println(paragraph);
            Result result = ToAnalysis.parse(paragraph);
            List<Term> parse = result.getTerms();

            StringBuilder all = new StringBuilder();
            for(Term t : parse){
                String name = t.getName();
                if(NameList.contains(name)){
                    all.append(name);
                    all.append(" ");
                }
            }
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            context.write(new Text(fileName), new Text(all.toString()));
        }
    }

    public static class WordConcurrenceReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            //System.out.println("Here:reduce\r");
            Iterator<Text> it = values.iterator();
            StringBuilder all = new StringBuilder();
            if(it.hasNext()){
                all.append(it.next().toString());
            }
            for(;it.hasNext();){
                all.append(it.next().toString());
            }
            //context.write(key, new Text(all.toSting()));
            String path_name = context.getConfiguration().get("path_write");
            File file = new File(path_name + "Task1" + key.toString());
            PrintStream ps = new PrintStream(new FileOutputStream(file));
            ps.println(all.toString());
        }
    }

    public static void main(String[] args) throws Exception{
        System.out.println("File Read Path:");
        Scanner sc1 = new Scanner(System.in);
        String path_r = sc1.nextLine();
        System.out.println("File Written Path:");
        Scanner sc2 = new Scanner(System.in);
        String path_w = sc2.nextLine();
        /*System.out.println("File of People's Name Path on HDFS:");  //absolute path without start named HDFS
        Scanner sc3 = new Scanner(System.in);
        String path_name = sc3.nextLine();*/

        Configuration conf = new Configuration();
        Job job = new Job(conf, "WordConcurrence");
        //job.getConfiguration().set("path_name", path_name);
        job.getConfiguration().set("path_write", path_w);
        job.setJarByClass(WordConcurrence.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordConcurrenceMapper.class);
        job.setReducerClass(WordConcurrenceReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(path_r));
        FileOutputFormat.setOutputPath(job, new Path(path_w));
        job.waitForCompletion(true);
    }
}
