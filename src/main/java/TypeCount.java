import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class TypeCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"typecount");
        job.setJarByClass(TypeCount.class);
        job.setReducerClass(TypeReducer.class);
        job.setMapperClass(TypeMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static class TypeMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            Text callType = new Text(line[12]);
            int[] callingOptr = new int[]{0,0,0};
            for (int i = 3; i <= 4; i++) {
                if("1".equals(line[i]) || "2".equals(line[i]) || "3".equals(line[i])){
                    callingOptr[Integer.parseInt(line[i]) - 1] = 1;
                }
            }
            StringBuilder result = new StringBuilder();
            for (int j : callingOptr) {
                result.append(j).append("\t");
            }
            context.write(callType,new Text(result.toString()));
        }
    }

    public static class TypeReducer extends Reducer<Text,Text,Text,Text>{
        DecimalFormat df = new DecimalFormat("0.00");//格式化小数，不足的补0
        @Override
        public void reduce(Text callType,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            int[] sums = new int[]{0,0,0};
            int total = 0;
            for (Text val : values) {
                String[] optrs = val.toString().split("\t");
                for (int i = 0; i < sums.length; i++) {
                    sums[i] += Integer.parseInt(optrs[i]);
                    total += Integer.parseInt(optrs[i]);
                }
            }
            StringBuilder result = new StringBuilder();
            for (int sum : sums) {
                if(total == 0){
                    result.append(0.00).append("\t");
                    continue;
                }
                result.append(df.format((double) sum/(double) total)).append("\t");
            }
            context.write(callType,new Text(result.toString()));
        }
    }

}
