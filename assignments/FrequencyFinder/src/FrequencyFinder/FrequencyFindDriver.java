package FrequencyFinder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by ajay on 6/15/16.
 */
public class FrequencyFindDriver {
    public static class FrequencyFindMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t");
            while(itr.hasMoreTokens()) {
                String str = itr.nextToken();
                for (char c: str.toCharArray()) {
                    if (Character.isLetter(c)) {
                        context.write(new Text(Character.toString(c)), one);
                    }
                }
            }
        }
    }


    /**
     * Created by ajay on 6/15/16.
     */
    public static class FrequencyFindReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val: values) {
                sum += val.get();
            }
            result = new IntWritable(sum);
            context.write(key, result);
        }
    }

    /**
     * Created by ajay on 6/18/16.
     */
    public class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable(Class<? extends Writable> valueClass) {
            super(valueClass);
        }

        public TextArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
            super(valueClass, values);
        }

    }
    /**
     * Created by ajay on 6/15/16.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FrequencyFinder");
        job.setJarByClass(FrequencyFindDriver.class);
        job.setMapperClass(FrequencyFindMapper.class);
        job.setReducerClass(FrequencyFindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
