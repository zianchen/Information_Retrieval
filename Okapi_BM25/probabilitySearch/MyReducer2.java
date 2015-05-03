import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;

  public  class MyReducer2
         extends Reducer<Text,Text,Text,Text> {

    private Text value1 = new Text();

    private HashMap<String,Integer>  relevantDocMap = globalVariables.relevantDocMap;
    private HashMap<String,Integer>  stoppedStemmedWordMap = globalVariables.stoppedStemmedWordMap;

    //private double moldQuery = Driver.moldQuery;
    private int numDocs = globalVariables.numDocs;
    private final double k_1 = 1.5;
    private final double k_3 = 1.5;
    private final double b = 0.75;

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //key : term,collectionFrequency,docFrequency
      String[] docIdMoldDocLenghtAveLength = key.toString().split(",");
      String docId = docIdMoldDocLenghtAveLength[0];
      //query map
      
      Integer value11 = relevantDocMap.get(docId);


      if (value11 != null) {

        for (Text val : values) {
          String termTfDfStr = val.toString();
          termTfDfStr = termTfDfStr.substring(1,termTfDfStr.length()-2);

          String[] termTfDfs = termTfDfStr.split(";");
          
          double rsv = 0.0;

          for(int i = 0; i < termTfDfs.length; i++){

            String[] termTfDfArray = termTfDfs[i].split(",");
            //it is query tf!
            Integer value22 = stoppedStemmedWordMap.get(termTfDfArray[0]);
            if (value22 != null) {     

              // double queryTfIdf = ( 1.0 +  Math.log10( stoppedStemmedWordMap.get(
              //   termTfDfArray[0]) ) ) * Math.log10( numDocs / Integer.parseInt(termTfDfArray[2]) ) ;

              // double docTfIdf =  ( 1.0 +  Math.log10(Integer.valueOf(termTfDfArray[1]))) * 
              //       Math.log10( numDocs / Integer.parseInt(termTfDfArray[2]) ) ;

              // tfIdf += queryTfIdf * docTfIdf;
              rsv += Math.log10( numDocs / Integer.parseInt(termTfDfArray[2]) ) * 
                    (k_1 + 1) * Integer.parseInt(termTfDfArray[1]) / (k_1 * (1 - b) + b * (Double.parseDouble(docIdMoldDocLenghtAveLength[2]) 
                      / Double.parseDouble(docIdMoldDocLenghtAveLength[3]) ) +  Integer.parseInt(termTfDfArray[1]) );

            }

          }
          //double normLizedTfIdf = tfIdf / (moldQuery * Double.valueOf(docIdMold[1])) ; 
          key.set(docId);
          value1.set( String.valueOf(rsv));
          context.write(key, value1);
        }
      }

  }
}
