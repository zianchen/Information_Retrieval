import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;

  public  class MyUniReducer4
         extends Reducer<Text,Text,Text,Text> {

    private Text word = new Text();

    private String avgDocLength = Integer.toString(globalVariables.avgDocLength);

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      for (Text val : values) {

        word.set(key.toString() + "," + avgDocLength );

        context.write(word, val);

      }
  }
}
