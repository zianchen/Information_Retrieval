import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Iterator;


public final class globalVariables 
{
   
    public final static int numDocs = 806791;

    public static HashMap<String, Integer> relevantDocMap;
    static {

        relevantDocMap = new HashMap<String,Integer>();
    
    }

    public static HashMap<String, Double> docScoreMap;
    static {

        docScoreMap = new HashMap<String,Double>();

    }

    public static HashMap<String, Integer> queryTermDfMap;
    static {

        queryTermDfMap = new HashMap<String,Integer>();
    
    }


    public static HashMap<String, Double> rankResultMap;
    //new is needed
    static {

        rankResultMap = new HashMap<String,Double>();
    
    }

    public static final HashMap<String, Integer> stoppedStemmedWordMap;
    static {
        stoppedStemmedWordMap = new HashMap<String,Integer>();
        try{   
            BufferedReader in = new BufferedReader(new FileReader("/home/wei6/wei6Project3/probabilitySearch/term"));
            String term = "";
            term = in.readLine();
            in.close();

            term = term.toLowerCase();
            term = Stopwords.removeStopWords(term);
            //can not have System.out.println
            String[] multiTerm = term.split("\\s+");
            englishStemmer stemmer2 = new englishStemmer();
            for(int i = 0; i < multiTerm.length; i++){
                stemmer2.setCurrent(multiTerm[i]);
                stemmer2.stem();
                String stoppedStemmedWord = stemmer2.getCurrent();
                Integer value = stoppedStemmedWordMap.get(stoppedStemmedWord);
                if (value != null) {
                  stoppedStemmedWordMap.put(stoppedStemmedWord, stoppedStemmedWordMap.get(stoppedStemmedWord) + 1);
                }
                else{
                  stoppedStemmedWordMap.put(stoppedStemmedWord,1);
                }
            }
        }catch(FileNotFoundException e){
            //System.out.println("Exception thrown : ", + e);
            e.printStackTrace();
        }
        catch(IOException e){
            //System.out.println("Can not open file term!");
            e.printStackTrace();
        }
    }

   



}
