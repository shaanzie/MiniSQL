package MapReduce;
import java.io.IOException;
import java.io.*;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MapReduce{
    public static class SelectMapper
       extends Mapper<Object, Text, Text, IntArrayWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException{
            String line = value.toString();

            IntArrayWritable lineNum = new IntArrayWritable(line[0].toInteger());
            context.write(new Text(line), lineNum);
            
        }
    }
    
    public static class SelectReducer
    extends Reducer<Text, IntArrayWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntArrayWritable> values,
                        org.apache.hadoop.mapreduce.Reducer<Text, IntArrayWritable, Text, Text>.Context context
                        ) throws IOException, InterruptedException {
            
                
            HashMap<Integer, Integer> seenBefore = new HashMap<Integer, Integer>();

            for (IntArrayWritable val : values) {
                Writable[] vals = val.get();
                seenBefore[vals[0]]++;
                if(seenBefore[vals[0]] == 1)
                {
                    result.set(new Text(vals));
                }
            }

        }
    }


    static class IntArrayWritable extends ArrayWritable {

        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(int[] integers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[integers.length];
            for (int i = 0; i < ints.length; i++) {
                ints[i] = new IntWritable(integers[i]);
            }
            set(ints);
        }
    }
    
}
