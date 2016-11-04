import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * Created by nina on 11/3/16.
 */
public class UnitSum {
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] subPR = value.toString().split("\t");
            double subPRVal = Double.parseDouble(subPR[1]);
            context.write(new Text(subPR[0]), new DoubleWritable(subPRVal));
        }
    }
    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum = sum + value.get();
            }
            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));
            context.write(key, new DoubleWritable(sum));
        }
    }
}
