import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

class PageRankComparator extends DoubleWritable {
    PageRankComparator(){super();}
    PageRankComparator(Double a){super(a);}

    //@Override
    public int compareTo(Object o){
        //System.out.println(o.getClass().toString());
        return -super.compareTo(o);
    }
}

public class PageRankViewer{

        public static class PageRankViewerMapper extends Mapper<LongWritable, Text, PageRankComparator, Text>{

        //private Text outPage = new Text();
        //private PageRankComparator outPr = new PageRankComparator();
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            //System.out.println(value.toString());
            String[] line = value.toString().split("\t");
            String page = line[0];
            Double pr = Double.parseDouble(line[1].split("#")[0]);
            //outPage.set(page);
            //outPr.set(pr);
            Text outPage = new Text(page);
            PageRankComparator outPr = new PageRankComparator(pr);
            context.write(outPr, outPage);
        }
    }

    public static class PageRankViewerReducer extends Reducer<PageRankComparator, Text, Text, Text>{

        protected void reduce(PageRankComparator key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text value : values){
                context.write(new Text(key.toString()), new Text(value));
            }
        }
    }



    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "PageRankViewer");

        job.setJarByClass(PageRankViewer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(PageRankViewerMapper.class);
        job.setReducerClass(PageRankViewerReducer.class);
        //job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(PageRankComparator.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}