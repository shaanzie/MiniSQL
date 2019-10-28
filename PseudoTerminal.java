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
import MapReduce.SelectJar;
import MapReduce.SelectMapper;
import MapReduce.SelectReducer;
public class PseudoTerminal{


    static void selectClause(String columns, String schema)
    {   
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "SelectJob");
        job1.setJarByClass(SelectJar.class);
        job1.setMapperClass(SelectMapper.class);
        
        job1.setReducerClass(SelectReducer.class);
        job1.setOutputKeyClass(Text.class);
    
        job1.setMapOutputValueClass(IntArrayWritable.class);


        Path schemaPath = new Path("/schema/" + schema);

        FileInputFormat.addInputPath(job1, schemaPath);
        FileOutputFormat.setOutputPath(job1, System.out);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);

        }
    }

    public static void main(String[] args) {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String input = "";
        
        try {
            while (!input.equalsIgnoreCase("stop")) {
                showMenu();
                input = in.readLine();
                
                // Parsing the input string

                String[] parsedArgs = input.split(" ");
                
                switch(parsedArgs[0])
                {
                    case "load":    
                        System.out.println("Loading");
                        break;
                    
                    case "select":
                        try {
                            String columns = parsedArgs[1];
                            String schema = parsedArgs[3]; 
                            selectClause(columns, schema);

                        } catch (Exception e) {
                            System.out.println("Syntax Error");    
                        }
                        break;
                    
                    case "delete":
                        System.out.println("Delete");
                        break;
                    
                    case "exit":
                        System.exit(0);
                        break;

                }
                
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void showMenu() {
        System.out.println("MiniSQL::>");
    }

}