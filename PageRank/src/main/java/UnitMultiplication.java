import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nina on 11/3/16.
 */
public class UnitMultiplication {
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input format : from\t to1,to2,to3
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");
            // dead end
            if (fromTo.length == 1 || fromTo[1].trim().equals("")) {
                return;
            }
            String from = fromTo[0];
            String[] tos = fromTo[1].split(",");
            for (String to : tos) {
                context.write(new Text(from), new Text(to + "=" + (double)1 / tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> transitionUnits = new ArrayList<String>();
            double prUnit = 0;
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transitionUnits.add(value.toString());
                } else {
                    prUnit = Double.parseDouble(value.toString());
                }
            }
            for (String transUnit : transitionUnits) {
                String outputKey = transUnit.split("=")[0];
                double transVal = Double.parseDouble(transUnit.split("=")[1]);
                String outputValue = String.valueOf(prUnit * transVal);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }


}
