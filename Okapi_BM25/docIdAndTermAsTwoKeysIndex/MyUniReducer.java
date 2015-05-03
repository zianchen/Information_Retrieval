import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;

  public  class MyUniReducer
         extends Reducer<Text,Text,Text,Text> {
  
    private Text value1 = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int tf = 0;

      for (Text val : values) {

        tf +=  Integer.parseInt ( val.toString() );

      }
      //use newKey
      String saveKey1 = key.toString();
      String saveKey2 = key.toString();
      
      key.set(saveKey1.replaceAll("[0-9,]",""));

      //use key since key has not been modified here
      value1.set(saveKey2.replaceAll("[A-Za-z,]", "") + "," +  Integer.toString(tf)
      );
      
      context.write(key, value1);
      
    }

  }
