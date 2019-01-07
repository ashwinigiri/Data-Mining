import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;

public class BlockMult{
    public static class MapperA extends Mapper<Object, Text, Text, Text> 
    {
        private Text outKey = new Text();
        private Text outVal = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] input = value.toString().replace("[","").replace("]","").replace("(","").replace(")","").split(",");
            String var = value.toString().replace("[","").replace("]","").replace("(","").replace(")","");
            String matrixName = "A";
            String content = var.substring(4,var.length());
			String i = input[0];
			String k = input[1];		
			String val="";
			for (int b = 1; b <=3 ; b++) 
			{
				String outputKey = "(" + i + "," + b + ")";
				outKey.set(outputKey);
				val= "(" + matrixName + "," + k + "," + content + ")";
				outVal.set(val);
				context.write(outKey,outVal);
				
			}
        }
	
	}
	
	public static class MapperB extends Mapper<Object, Text, Text, Text> 
    {
        private Text outKey = new Text();
        private Text outVal = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] input = value.toString().replace("[","").replace("]","").replace("(","").replace(")","").split(",");
            String var = value.toString().replace("[","").replace("]","").replace("(","").replace(")","");
            String matrixName = "B";
            String content = var.substring(4,var.length());
            String k = input[0];
			String j = input[1];
		    String val="";
			for (int b = 1; b <=3 ; b++) 
            {
				String outputKey = "(" + b + "," + j + ")";
				outKey.set(outputKey);
				val= "(" + matrixName + "," + k + "," + content + ")";
				outVal.set(val);
				context.write(outKey,outVal);
				
			}
		}
    }

	public static class Reduce extends Reducer<Text, Text, Text, Text> 
    {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{	
            List<String> list1 = new ArrayList<String>();
            List<String> list2 = new ArrayList<String>();
            List<String> list3 = new ArrayList<String>();
            int c1[][]=new int[2][2];
            int c2[][]=new int[2][2];
            int c3[][]=new int[2][2];
            int c[][]=new int[2][2];
            for (Text val : values) 
            { 
                String value = val.toString();
                String check=value.substring(3,4);
                if(check.equals("1"))
                {   
                    list1.add(value); 
                }
                else if(check.equals("2"))
                {
                    list2.add(value);
                }
                else if(check.equals("3"))
                {
                    list3.add(value);
                }
            }
           
            if(list1.size()==2)
            {
                String one[] = list1.get(0).toString().replace("(","").replace(")","").split(",");
                String two[] = list1.get(1).toString().replace("(","").replace(")","").split(",");
                int A[][]=new int[2][2];
                int B[][]=new int[2][2];
                if(one[0].equals("A"))
                {
                    for (int i=2; i < one.length; i+=3)
                    {
                        int z1= Integer.parseInt(one[i]);
                        int z2= Integer.parseInt(one[i+1]);
                        int z3= Integer.parseInt(one[i+2]);
                        A[z1-1][z2-1] = z3;
                    }
                }
                if(one[0].equals("B"))
                {
                    for (int i=2; i < one.length; i+=3)
                    {
                        int z1= Integer.parseInt(one[i]);
                        int z2= Integer.parseInt(one[i+1]);
                        int z3= Integer.parseInt(one[i+2]);
                        B[z1-1][z2-1] = z3; 
                    }
                } 
                if(two[0].equals("A"))
                {
                    for (int i=2; i < two.length; i+=3)
                    {
                        int z1= Integer.parseInt(two[i]);
                        int z2= Integer.parseInt(two[i+1]);
                        int z3= Integer.parseInt(two[i+2]);
                        A[z1-1][z2-1] = z3;
                    }
                }      
                if(two[0].equals("B"))
                {
                    for (int i=2; i < two.length; i+=3)
                    {
                        int z1= Integer.parseInt(two[i]);
                        int z2= Integer.parseInt(two[i+1]);
                        int z3= Integer.parseInt(two[i+2]);
                        B[z1-1][z2-1] = z3; 
                    }
                }
                for(int i=0;i<2;i++)
                {
                    for(int j=0;j<2;j++)
                    {
                        for (int k=0; k<2;k++)
                        {
                            c1[i][j] += A[i][k] * B[k][j];
                        }
                    }
                }        
            }
            if(list2.size()==2)
            {
                String one[] = list2.get(0).toString().replace("(","").replace(")","").split(",");
                String two[] = list2.get(1).toString().replace("(","").replace(")","").split(",");
                int A[][]=new int[2][2];
                int B[][]=new int[2][2];
                if(one[0].equals("A"))
                {
                    for (int i=2; i < one.length; i+=3)
                    {
                        int z1= Integer.parseInt(one[i]);
                        int z2= Integer.parseInt(one[i+1]);
                        int z3= Integer.parseInt(one[i+2]);
                        A[z1-1][z2-1] = z3;
                    }
                }
                if(one[0].equals("B"))
                {
                    for (int i=2; i < one.length; i+=3)
                    {
                        int z1= Integer.parseInt(one[i]);
                        int z2= Integer.parseInt(one[i+1]);
                        int z3= Integer.parseInt(one[i+2]);
                        B[z1-1][z2-1] = z3;
                    }
                }   
                if(two[0].equals("A"))
                {
                    for (int i=2; i < two.length; i+=3)
                    {
                        int z1= Integer.parseInt(two[i]);
                        int z2= Integer.parseInt(two[i+1]);
                        int z3= Integer.parseInt(two[i+2]);
                        A[z1-1][z2-1] = z3;
                    }
                }
                if(two[0].equals("B"))
                {
                    for (int i=2; i < two.length; i+=3)
                    {
                        int z1= Integer.parseInt(two[i]);
                        int z2= Integer.parseInt(two[i+1]);
                        int z3= Integer.parseInt(two[i+2]);
                        B[z1-1][z2-1] = z3;
                    }
                }
                for(int i=0;i<2;i++)
                {
                    for(int j=0;j<2;j++)
                    {
                        for (int k=0; k<2;k++)
                        {
                            c2[i][j] += A[i][k] * B[k][j];
                        }
                    }
                }           
            }
            if (list3.size()==2)
            {
                String one[] = list3.get(0).toString().replace("(","").replace(")","").split(",");
                String two[] = list3.get(1).toString().replace("(","").replace(")","").split(",");
                int A[][]=new int[2][2];
                int B[][]=new int[2][2]; 
                if(one[0].equals("A"))
                {
                    for (int i=2; i < one.length; i+=3)
                    {
                        int z1= Integer.parseInt(one[i]);
                        int z2= Integer.parseInt(one[i+1]);
                        int z3= Integer.parseInt(one[i+2]);
                        A[z1-1][z2-1] = z3;
                    }
                }
                if(one[0].equals("B"))
                {
                    for (int i=2; i < one.length; i+=3)
                    {
                        int z1= Integer.parseInt(one[i]);
                        int z2= Integer.parseInt(one[i+1]);
                        int z3= Integer.parseInt(one[i+2]);
                        B[z1-1][z2-1] = z3;
                    }
                }
                if(two[0].equals("A"))
                {
                    for (int i=2; i < two.length; i+=3)
                    {
                        int z1= Integer.parseInt(two[i]);
                        int z2= Integer.parseInt(two[i+1]);
                        int z3= Integer.parseInt(two[i+2]);
                        A[z1-1][z2-1] = z3;
                    }
                }
                if(two[0].equals("B"))
                {
                    for (int i=2; i < two.length; i+=3)
                    {
                        int z1= Integer.parseInt(two[i]);
                        int z2= Integer.parseInt(two[i+1]);
                        int z3= Integer.parseInt(two[i+2]);
                        B[z1-1][z2-1] = z3;
                    }
                }
                for(int i=0;i<2;i++)
                {
                    for(int j=0;j<2;j++)
                    {
                        for (int k=0; k<2;k++)
                        {
                            c3[i][j] += A[i][k] * B[k][j] ;
                        }
                    }
                }
            }
            for (int r = 0 ; r < 2 ; r++ )
            {
                for ( int s = 0 ; s< 2 ; s++ )
                {
                    c[r][s] = c1[r][s] + c2[r][s] + c3[r][s];
                }
            }
            String output = "";
            for(int i=0;i<2;i++)
            {
                for(int j=0;j<2;j++)
                {
                    if(c[i][j]!=0)
                        {
                            output+="("+(i+1)+","+(j+1)+","+c[i][j]+")"+",";
                        } 
                }
            }
			if(!output.equals(""))
            {
                Text outputValue = new Text();
                outputValue.set(",["+output.substring(0,output.length()-1)+"]");
                context.write(key,outputValue);
		    }	
        }	
    }
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) 
        {
            System.err.println("Error");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "BlockMult");
        job.setJarByClass(BlockMult.class);
        job.setMapperClass(MapperA.class);
        job.setMapperClass(MapperB.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(MultipleInputs.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path path1=new Path(otherArgs[0]);
	    Path path2=new Path(otherArgs[1]);
        MultipleInputs.addInputPath(job, path1, TextInputFormat.class, MapperA.class);
        MultipleInputs.addInputPath(job, path2, TextInputFormat.class, MapperB.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

