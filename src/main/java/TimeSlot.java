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

public class TimeSlot {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"timesperday");
        job.setJarByClass(TimeSlot.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(TimeMapper.class);
        job.setReducerClass(TimeReduce.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
    }

    public static class TimeMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String startTime = line.split("\t")[9];
            String time = line.split("\t")[11];
            Text phoneNum = new Text(line.split("\t")[1]);
            Text slot = new Text(toSlot(startTime,time));
            context.write(phoneNum,slot);
        }
    }

    public static class TimeReduce extends Reducer<Text,Text,Text,Text>{
        DecimalFormat df = new DecimalFormat("0.000");//格式化小数，不足的补0

        @Override
        public void reduce(Text phoneNum, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] sums = new int[]{0,0,0,0,0,0,0,0};
            int total = 0;
            for (Text val : values) {
                String[] slots = val.toString().split("\t");
                for (int i = 0; i < sums.length; i++) {
                    sums[i] += Integer.parseInt(slots[i]);
                    total += Integer.parseInt(slots[i]);
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
            context.write(phoneNum,new Text(String.valueOf(result)));
        }
    }

    public static String toSlot(String startTime, String time){
        int hours = Integer.parseInt(startTime.split(":")[0]);
        int minutes = Integer.parseInt(startTime.split(":")[1]);
        int seconds = Integer.parseInt(startTime.split(":")[2]);
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < 24; i+= 3) {
            if(i <= hours && hours < i + 3){
                int tmp = (i + 3 - hours) * 60 * 60 + (60 - minutes) * 60 + 60 -seconds;
                if(tmp >= Integer.parseInt(time)){
                    result.append(time).append("\t");
                }
                else {
                    result.append(tmp).append("\t");
                    hours = i + 3;
                    minutes = 0;
                    seconds = 0;
                    time = String.valueOf(Integer.parseInt(time) - tmp);
                }
            }
            else{
                result.append(0).append("\t");
            }
        }
        return result.toString();
    }
}
