import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Rating implements Writable {

    public int movieID;
    public int rating;

    Rating () {}

    Rating (int m, int r) {
        movieID = m;
        rating = r;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(movieID);
        out.writeInt(rating);
    }

    public void readFields ( DataInput in ) throws IOException {
        movieID = in.readInt();
        rating = in.readInt();
    }

}

class Title implements Writable {

    public int movieID;
    public String year;
    public String title;

    Title () {}

    Title (int m, String y, String t) {
        movieID = m;
        year = y;
        title = t;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(movieID);
        out.writeUTF(year);
        out.writeUTF(title);
    }

    public void readFields ( DataInput in ) throws IOException {
        movieID = in.readInt();
        year = in.readUTF();
        title = in.readUTF();
    }

}

class RatingTitle implements Writable {
    public short tag;
    public Rating rating;
    public Title title;

    RatingTitle () {}
    RatingTitle ( Rating r ) { tag = 0; rating = r; }
    RatingTitle ( Title t ) { tag = 1; title = t; }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(tag);
        if (tag==0)
            rating.write(out);
        else title.write(out);
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readShort();
        if (tag==0) {
            rating = new Rating();
            rating.readFields(in);
        } else {
            title = new Title();
            title.readFields(in);
        }
    }
}

class Result implements Writable {
    public float rating;
    public String title;

    /* put your code here */

    Result () {}

    Result ( float r, String t ) {
        rating = r;
        title = t;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeFloat(rating);
        out.writeUTF(title);
    }

    public void readFields ( DataInput in ) throws IOException {
        rating = in.readFloat();
        title = in.readUTF();
    }

    public String toString () {
        return " "+ title + "  "+ rating;
    } 
}

public class Netflix {

    /* put your code here */
    public static class RatingMapper extends Mapper<Object,Text,IntWritable,RatingTitle > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            String str[] = value.toString().split(",");
            int movieID = Integer.parseInt(str[0]);
            int rating = Integer.parseInt(str[2]);
            Rating r = new Rating(movieID, rating);
            context.write(new IntWritable(movieID),new RatingTitle(r));
        }
    }

    public static class TitleMapper extends Mapper<Object,Text,IntWritable,RatingTitle > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
                String str[] = value.toString().split(",");
                int movieID = 0;
                //int year = 0;
                // if(str[0].equalsIgnoreCase("")){
                    movieID = Integer.parseInt(str[0]);
                //}
                //if(str[1].equalsIgnoreCase("")){
                   // year = str[1];
                //}
                //if(str[2].equalsIgnoreCase("NULL")){
                    Title t = new Title(movieID,str[1],str[2]);
                    context.write(new IntWritable(movieID),new RatingTitle(t));
                //} 
        }
    }

    public static class ResultReducer extends Reducer<IntWritable,RatingTitle,Text,Result> {
        static Vector<Rating> r = new Vector<Rating>();
        static Vector<Title> t = new Vector<Title>();
        @Override
        public void reduce ( IntWritable key, Iterable<RatingTitle> values, Context context )
                           throws IOException, InterruptedException {
            r.clear();
            t.clear();
            int count = 0;
            float sum = 0;
            String year = "";
            String title = "";
            boolean flag = false;
            for (RatingTitle v: values)
                if (v.tag == 0)
                    r.add(v.rating);
                else t.add(v.title);
            for ( Title d: t ){
                for ( Rating e: r ){
                    flag = true;
                    sum = sum + e.rating;
                    count++;
                    year = d.year;
                    title = d.title;
                }  
                if(flag){
                    sum = sum / count;
                    String result = year + ":  ";
                    context.write(new Text(result),new Result(sum,title));
                    flag = false;
                }
            }
                
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* put your code here */

        Job job = Job.getInstance();
        job.setJobName("JoinJob");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Result.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RatingTitle.class);
        job.setReducerClass(ResultReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,RatingMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,TitleMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

    }
}

