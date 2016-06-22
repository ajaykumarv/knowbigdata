import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.StringTokenizer;



/**
 * Created by ajay on 6/15/16.
 */
public class FindPeopleSameDNADriver2 {
    public static class FindPeopleSameDNAMapper extends Mapper<Object, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " \t\"");
            Text username = null;
            Text dna_seq = null;
            if (itr.hasMoreTokens()) {
                username = new Text(itr.nextToken());
            }
            if (itr.hasMoreTokens()) {
                dna_seq = new Text(itr.nextToken());
            }
            char[] arr = dna_seq.toString().toCharArray();
            for (int index = 0; index < arr.length/2; index++) {
                char temp = arr[index];
                arr[index] = arr[arr.length-1-index];
                arr[arr.length - 1 -index] = temp;
            }
            String dna_seq_rev = String.valueOf(arr);
            if (dna_seq != null && username != null){

                if (dna_seq.toString().compareTo(dna_seq_rev) <= 0) {
                    context.write(dna_seq, username);
                } else {
                    context.write(new Text(dna_seq_rev), username);
                }
            }
        }
    }


    /**
     * Created by ajay on 6/15/16.
     */
    public static class FindPeopleSameDNAReducer extends Reducer<Text, Text, Text, TextArrayWritable> {
        private IntWritable result;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            HashSet<String> mySet = new HashSet<String>();

            for(Text val: values) {
                mySet.add(val.toString());
            }

            Object[] myArray = mySet.toArray();

            Text[] str_arr = new Text[myArray.length];
            int i = 0;
            for(Object s: myArray) {
                str_arr[i] = new Text(s.toString());
                i += 1;
            }
            TextArrayWritable text_arr = new TextArrayWritable(Text.class, str_arr);
            context.write(key, text_arr);
        }
    }

    /**
     * Created by ajay on 6/18/16.
     */
    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable(Class<? extends Writable> valueClass) {
            super(valueClass);
        }

        public TextArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
            super(valueClass, values);
        }

        @Override
        public String toString() {
            Text[] strings = (Text[] )super.get();
            String output = "";
            for (Text t: strings) {
                output = t.toString() + " " + output;
            }
            return output;
        }
    }

    /**
     * Created by ajay on 6/15/16.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FindPeopleSameDNA");
        job.setJarByClass(FindPeopleSameDNADriver2.class);
        job.setMapperClass(FindPeopleSameDNAMapper.class);
        job.setReducerClass(FindPeopleSameDNAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
