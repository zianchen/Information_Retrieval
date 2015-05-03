import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;

  public  class MyReducer
         extends Reducer<Text,Text,Text,Text> {

    private Text value1 = new Text();
    private HashMap<String,Integer>  stoppedStemmedWordMap = globalVariables.stoppedStemmedWordMap;

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
     
      String[] termDf = key.toString().split(",");
      String term = termDf[0];
      Integer value = stoppedStemmedWordMap.get(term);
      
      if (value != null) {

        Integer value11 = globalVariables.queryTermDfMap.get(term);
        if(value11 == null){
          globalVariables.queryTermDfMap.put(termDf[0], Integer.valueOf(termDf[1]));
        }

        for (Text val : values) {
          String docs = val.toString();
          docs = docs.substring(1,docs.length()-2);
          String[] docsArray= docs.split(";");
          
          for(int i = 0; i < docsArray.length; i++){
           
            String docId = docsArray[i];
            
            Integer value22 = globalVariables.relevantDocMap.get(docId);
          
            if(value22 == null){
              //any number can be here
              globalVariables.relevantDocMap.put(docId,1);
            }

            key.set(docId);
            value1.set(term);

            context.write(key, value1);

          }

        }

      }
    }
  
  }