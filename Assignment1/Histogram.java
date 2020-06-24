import java.io.*;
import java.net.URI;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable<Color> {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */
    
	public Color(short type, short intensity) {
		this.type = type;
		this.intensity = intensity;
	}
	public Color() {
		this.type = 0;
		this.intensity = 0;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeShort(type);
		out.writeShort(intensity);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		type = in.readShort();
		intensity = in.readShort();
		
	}
	@Override
	public int compareTo(Color o) {
		return ((this.type==o.type)? (int)(this.intensity-o.intensity) : (int)(this.type-o.type));
	}
	
	@Override
	public String toString() {
		return type +" "+intensity;
	}
	
}

public class Histogram {
	static Vector<Color> colorVector = new Vector<Color>();
	
    public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {

        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            /* write your mapper code */

        	Scanner scan = new Scanner(value.toString()).useDelimiter(",");
        	short red, green, blue;
        	red = scan.nextShort();
        	green = scan.nextShort();
        	blue = scan.nextShort();
        	context.write(new Color((short)1,red), new IntWritable(1));
        	context.write(new Color((short)2,green), new IntWritable(1));
        	context.write(new Color((short)3,blue), new IntWritable(1));
        	scan.close();

        }
    }

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
        	long count =0;
        	for( IntWritable i : values) {
        		count+= i.get();
        	}
        	context.write(key, new LongWritable(count));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* write your main program code */
    	Job job = Job.getInstance();
		job.setJobName("HistogramMapReduce");
		job.setJarByClass(Color.class);

		//name reducer and mapper classes
		job.setReducerClass(HistogramReducer.class);
		job.setMapperClass(HistogramMapper.class);

		//Set output Key and value classes
		job.setMapOutputKeyClass(Color.class);
		job.setOutputKeyClass(Color.class);

		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//Set Input and Output Paths

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);
   }
}
