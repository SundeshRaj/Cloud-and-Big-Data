import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import java.lang.*;
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
        VID = (long) 0;
        adjacent = new Vector<Long>();
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
        LongWritable s = new LongWritable(adjacent.size());
        s.write(out);

        for(Long v: adjacent) {
            out.writeLong(v);
        }
    }

    public void readFields ( DataInput in ) throws IOException {

        tag = in.readShort();
        group = in.readLong();
        VID = in.readLong();
        adjacent = new Vector<Long>();
        adjacent.clear();
        LongWritable size = new LongWritable();
        size.readFields(in);
        for(long i=0; i < size.get(); i++) {
            LongWritable a1 = new LongWritable();
            a1.readFields(in);
            adjacent.add(a1.get());
        }
    }
}


public class Graph{
    public static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Vector<Long> adjacent = new Vector<Long>();
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long VID = s.nextLong();
            while (s.hasNext()) {
                long ad = s.nextLong();
                adjacent.add(ad); // the vertex neighbors
            }
            short gid = 0;
            context.write(new LongWritable(VID),new Vertex(gid,VID,VID,adjacent));
            s.close();
        }
    }
    public static class SecondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
        //send graph topology
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
            long m = Long.MAX_VALUE;
            short t = 1;
            long kk = key.get();
            Vector<Long> adj = new Vector<Long>();
            for (Vertex v: values) {
                if (v.tag == 0) {
                    adj = (Vector)v.adjacent.clone();
                }
                if (v.group < m )
                    m = v.group;
            }
            short j = 0;
            long k = key.get();
            context.write(new LongWritable(m),new Vertex (j,m,k,adj));
        }
    }
    public static class FinalMapper extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                throws IOException, InterruptedException {
            long i = 1;
            context.write(key,new LongWritable(i));
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
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJobName("GraphJob");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setMapperClass(FirstMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        boolean run1 = job.waitForCompletion(true);
        boolean run2 = false;

        if(run1){
            for (short i = 0; i<5; i++) {
                job = Job.getInstance();
                job.setJobName("GraphJob2");
                job.setJarByClass(Graph.class);
                job.setOutputKeyClass(LongWritable.class);
                job.setOutputValueClass(Vertex.class);
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(Vertex.class);
                job.setMapperClass(SecondMapper.class);
                job.setReducerClass(SecondReducer.class);
                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
                //MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MMapper.class);
                //MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,NMapper.class);
                FileInputFormat.setInputPaths(job, new Path(args[1]+"/f"+i));
                FileOutputFormat.setOutputPath(job,new Path(args[1]+"/f"+(i+1)));
                run2 = job.waitForCompletion(true);
                if(!run2)
                    break;

            }
        }
        if(run2){
            job = Job.getInstance();
            job.setJobName("GraphJob2");
            job.setJarByClass(Graph.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);  // Long?
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setMapperClass(FinalMapper.class);
            job.setReducerClass(FinalReducer.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job,new Path(args[1]+"/f5"));
            FileOutputFormat.setOutputPath(job,new Path(args[2]));


            job.waitForCompletion(true);
        }
    }
}