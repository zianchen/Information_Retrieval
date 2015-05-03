import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import java.util.ArrayList;
import java.util.List;

  public  class MyUniReducer2
         extends Reducer<Text,Text,Text,Text> {

    MultipleOutputs<Text, Text> mos;

    public void setup(Context context) {
        //mos = new MultipleOutputs<Text, Text>(context);
        mos = new MultipleOutputs<Text,Text>(context);
    } 

    private Text word = new Text();
    private Text value1 = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      List<String> cache = new ArrayList<String>();

      String value = "<";
      for(Text val : values) {
        value += val.toString().split(",")[0] + ";" ;
        //for computing df
        cache.add(val.toString());
      }
      value += ">";


      String termAsKeyOutputFile = "termAsKeyOutputFile";


      int df = cache.size();
      

      for(int i=0; i < cache.size(); i++) {
        //not forget to plus ,

        String[] docIDTf = cache.get(i).split(",");
        
        word.set(docIDTf[0]);

        value1.set(key.toString() + "," + docIDTf[1] + "," + Integer.toString(df));

        //mos.write(termAsKeyOutputFile, word, value1);
        context.write(word,value1);
      }


      String termAsKeyIndex = "termAsKeyIndex";
      key.set( key.toString() + "," + Integer.toString(df) );
      //value was got in the first iteration
      value1.set(value);
      mos.write(termAsKeyIndex, key, value1);

    }
    //this override must be included, or data lose will come up
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
  }
