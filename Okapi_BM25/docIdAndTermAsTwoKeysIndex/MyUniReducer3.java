import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;

  public  class MyUniReducer3
         extends Reducer<Text,Text,Text,Text> {

    private Text value1 = new Text();
    private int avgDocLength = globalVariables.avgDocLength;
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int df = 0;

      String value = "<";

      double mold = 0;

      int numDocs = globalVariables.numDocs;
      int docLength = 0;
      for (Text val : values) {

        String[] termTfDf = val.toString().split(",");
        mold += ( (1.0 + Math.log10(Integer.valueOf(termTfDf[1])) ) * Math.log10 ( numDocs / Integer.valueOf(termTfDf[2]) )) *
                ( (1.0 + Math.log10(Integer.valueOf(termTfDf[1])) ) * Math.log10 ( numDocs / Integer.valueOf(termTfDf[2]) )) ;
        
        docLength += Integer.valueOf(termTfDf[1]);

        value += val.toString() + ";";

      }

      globalVariables.totalDocLength += docLength;

      mold = Math.sqrt(mold);

      value +=  ">";

      key.set(key.toString() + "," + Double.toString(mold) + "," + Integer.toString(docLength));

      value1.set(value);
      
      context.write(key, value1);
    }
  }
