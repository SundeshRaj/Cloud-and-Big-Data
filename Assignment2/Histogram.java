import java.io.*;
import java.util.HashMap;
import java.util.Objects;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Color implements WritableComparable<Color>  {
	
    public short type;        //red=1, green=2, blue=3 
    public short intensity;   //between 0 and 255 
     //need class constructors, toString, write, readFields, and compareTo methods 
    
	public Color(short type, short intensity) {
		this.type = type;
		this.intensity = intensity;
	}
	
	public Color() {
		this.type = 0;
		this.intensity = 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} 
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		Color col = (Color)obj;
		return type == col.type && intensity == col.intensity;
	}
	
	@Override 
	public int hashCode(){
		return Objects.hash(type,intensity);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeShort(type);
		out.writeShort(intensity);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
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
            // write your mapper code 
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
    public static class HistogramCombiner extends Reducer<Color,IntWritable,Color,IntWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            // write your reducer code 
        	int count =0;
        	for( IntWritable i : values) {
        		count+= i.get();
        	}
        	context.write(key, new IntWritable(count));
        }
    }
    
    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
             //write your reducer code 
        	long count =0;
        	
        	for( IntWritable i : values) {
        		count+= i.get();
        	}
        	context.write(key, new LongWritable(count));
        }
    }
    
    public static class HistogramMapper2 extends Mapper<Object,Text,Color,IntWritable> {
    	
    	static HashMap<Color,IntWritable> histogramHash = new HashMap<Color, IntWritable>();
    	@Override
    	protected void setup (Context context) throws IOException, InterruptedException {
    		histogramHash.clear();
    	}
    	
    	@Override
    	protected void cleanup (Context context) throws IOException, InterruptedException {
    		
    		for (Color c : histogramHash.keySet()) {
    			context.write(c, histogramHash.get(c));
    		}
    		histogramHash.clear();
    	}
    	
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
        	Scanner scan = new Scanner(value.toString());
        	scan.useDelimiter(",");
        	short red=0, green=0, blue=0;
        	red = scan.nextShort();
        	green = scan.nextShort();
        	blue = scan.nextShort();
        	Color redColor = new Color((short)1,red);
        	Color greenColor = new Color((short)2,green);
        	Color blueColor = new Color((short)3,blue);
        	
        	//red color
        	if(histogramHash.containsKey(redColor)){
        		histogramHash.replace(redColor, new IntWritable(histogramHash.get(redColor).get()+1));
        	}
        	else{
        		histogramHash.put(redColor, new IntWritable(1));
        	}
        	
        	//green color
        	if(histogramHash.containsKey(greenColor)){
        		histogramHash.replace(greenColor, new IntWritable(histogramHash.get(greenColor).get()+1));
        	}else{
        		histogramHash.put(greenColor, new IntWritable(1));
        	}
        	
        	//blue color
        	if(histogramHash.containsKey(blueColor)){
        		histogramHash.replace(blueColor, new IntWritable(histogramHash.get(blueColor).get()+1));
        	}else{
        		histogramHash.put(blueColor, new IntWritable(1));
        	}
        	scan.close();
        }
    }

    public static class HistogramReducer2 extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
             //write your reducer code 
        	long count =0;
        	
        	for( IntWritable i : values) {
        		count+= i.get();
        	}
        	context.write(key, new LongWritable(count));
        }
    }

    public static void main ( String[] args ) throws Exception {
         //write your main program code 
    	Job job = Job.getInstance();
		job.setJobName("HistogramMapReduce");
		job.setJarByClass(Color.class);

		//name reducer and mapper classes
		job.setReducerClass(HistogramReducer.class);
		job.setMapperClass(HistogramMapper.class);
		job.setCombinerClass(HistogramCombiner.class);

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
		
		
		Job job2 = Job.getInstance();
		job2.setJobName("HistogramMapReduce2");
		job2.setJarByClass(Color.class);

		//name reducer and mapper classes
		job2.setReducerClass(HistogramReducer2.class);
		job2.setMapperClass(HistogramMapper2.class);

		//Set output Key and value classes
		job2.setMapOutputKeyClass(Color.class);
		job2.setOutputKeyClass(Color.class);

		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputValueClass(LongWritable.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		//Set Input and Output Paths
		FileInputFormat.setInputPaths(job2,new Path(args[0]));
		FileOutputFormat.setOutputPath(job2,new Path(args[1]+"2"));
		job2.waitForCompletion(true);
   }
}