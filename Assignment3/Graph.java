import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import java.lang.Math;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors

    Vertex () {}
    Vertex (short t, long g) {
    	tag = t;
    	group = g;
    }
    Vertex (short t, long g, long v, Vector<Long> a) {
    	tag = t;
    	group = g;
    	VID = v;
    	adjacent = a;
    }
    
    public void write ( DataOutput out ) throws IOException {
			out.writeShort(tag); 
			out.writeLong(group);
			out.writeLong(VID);
    } 

    public void readFields ( DataInput in ) throws IOException {
			tag = in.readShort();
			group = in.readLong();
			VID = in.readLong();
    } 
}


public class Graph extends Configured implements Tool{
    	static Vector<Long> adjacent = new Vector<Long>();
		public static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex> {
  	//adjacent.clear();
       @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
		adjacent.clear();          
  Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long VID = s.nextLong();
            while (s.hasNext()) {
		long ad = s.nextLong();
            	adjacent.addElement(ad); // the vertex neighbors
            }
            short gid = 0;
	        context.write(new LongWritable(VID),new Vertex(gid,VID,VID,adjacent));
            s.close();
        }
    } 

//    public static class FirstMapper extends Mapper<Object,Text,IntWritable,Elem > {
//        @Override
//        public void map ( Object key, Text value, Context context )
//                        throws IOException, InterruptedException {
//            Scanner s = new Scanner(value.toString()).useDelimiter(",");
//            int i = s.nextInt();
//            int j = s.nextInt();
//			double v = s.nextDouble();
//	short t = 1;		
//	//Elem e = new Elem(s.nextInt(),s.nextInt(),s.nextDouble());
//            context.write(new IntWritable(i),new Elem(t,j,v));
//            s.close();
//        }
//    } 	


    
    public static class SecondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
    	
    	@Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
           
    		context.write(new LongWritable(value.VID),value);
            for (long n : value.adjacent) {
                      	short i = 1;
            	context.write(new LongWritable(n), new Vertex(i,value.group));
            }
      
        }
    }
    public static class SecondReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
	@Override     
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            //Vector<Long> adj = new Vector<Long>();
	    long m = Long.MAX_VALUE;
	    short t = 1;
  	    Vertex adj=new Vertex(t,long(0),long(0),new Vector<Long>());          
//double sum = 0;
            for (Vertex v: values) {
            	if (v.tag == 0) {
            		adj.adjacent = v.adjacent;
            	}
		if (v.group < m )
             	   m = v.group;
            	//sum = sum + v.get();
            }
            short j = 0;
	    long k = key.get();
            context.write(new LongWritable(m),new Vertex (j,m,k,adj));
        }
    }		
	    
    public static class FinalMapper extends Mapper<LongWritable,Vertex,LongWritable,IntWritable> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            short i = 1;
        	context.write(key,new IntWritable(i));
      
        }
    }
    public static class FinalReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {

            long m = 0;
            for (LongWritable v: values) {
		
                m = m + v.get();
            }
            context.write(key,new LongWritable(m));
        }
    }		
	
	@Override
    public int run(String [] args) throws Exception {
        Configuration conf = getConf();
		Job job = Job.getInstance(conf,"Job1");
        job.setJobName("GraphJob");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setMapperClass(FirstMapper.class);
        // job.setReducerClass(FirstReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MMapper.class); 
        //MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,NMapper.class);
       
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        job.waitForCompletion(true);
    
        for (short i = 0; i<5; i++) {
        Job job1 = Job.getInstance(conf,"Job2");
        job1.setJobName("GraphJob2");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setMapperClass(SecondMapper.class);
        job1.setReducerClass(SecondReducer.class);
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        //MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MMapper.class); 
        //MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,NMapper.class);
        FileInputFormat.setInputPaths(job1, new Path(args[1]+"/f"+i));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f"+(i+1)));
        job1.waitForCompletion(true);
        
        } 
	Job job2 = Job.getInstance(conf,"Job2");
        job2.setJobName("MultiplyJob2");
	job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(FinalMapper.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        
	
              //job2.waitForCompletion(true);
	return job2.waitForCompletion(true) ? 0 : 1;
   
    }
    public static void main ( String[] args ) throws Exception {
        ToolRunner.run(new Configuration(),new Graph(),args);
    }

}

