import java.io.IOException;
import java.lang.InterruptedException;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import java.io.FileReader;
import java.util.Iterator;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.File;

public class PageRank{

	static HashMap<String,String>  docTermMap = new HashMap<String,String>();
	static HashMap<String,String>  docMoldMap = new HashMap<String,String>();
	static HashMap<String,Integer>  stoppedStemmedWordMap = new HashMap<String,Integer>();
	static HashMap<String,Double>  docScoreMap = new HashMap<String,Double>();
	static HashMap<String,Integer>  queryIDFMap = new HashMap<String,Integer>();

	static int numDocs = 806791;

	private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap) {
 
		// Convert Map to List
		List<Map.Entry<String, Double>> list = 
			new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());
 
		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> o2,
                                           Map.Entry<String, Double> o1) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});
 
		// Convert sorted map back to a Map
		Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		for (Iterator<Map.Entry<String, Double>> it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Double> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	public static void main(String[] args) throws IOException,InterruptedException {

		BufferedReader in = new BufferedReader(new FileReader("./output/part-r-00000"));
		String line;
		
		while((line = in.readLine()) != null){
		    //System.out.println(line);
	        String[] docIDTermTF = line.split("\\s+");
	        String docID = docIDTermTF[0];

            String value1 = docTermMap.get(docID);
            if (value1 != null) {
              docTermMap.put(docID, docTermMap.get(docID) + ";" + docIDTermTF[1]);
            }
            else{
              docTermMap.put(docID, docIDTermTF[1]);
            }

		}
		in.close();

		BufferedReader in2 = new BufferedReader(new FileReader("./moldOutput/part-r-00000"));
		String line2;
		
		while((line2 = in2.readLine()) != null){
	        String[] docIDMold = line2.split("\\s+");
            docMoldMap.put(docIDMold[0], docIDMold[1]);

		}
		in2.close();
		
		//
		BufferedReader in3 = new BufferedReader(new FileReader("./term"));
		String term = in3.readLine();
		in3.close();
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

		BufferedReader in4 = new BufferedReader(new FileReader("./queryIDFOutput/part-r-00000"));
		String line4;
		
		while((line4 = in4.readLine()) != null){
		    //System.out.println(line);
	        String[] queryTermIDF = line4.split("\\s+");
            queryIDFMap.put(queryTermIDF[0], Integer.parseInt(queryTermIDF[1]) );

		}
		in2.close();

		double moldQuery = 0;
		Iterator itr = stoppedStemmedWordMap.entrySet().iterator();	
		while (itr.hasNext()) {
			Map.Entry pair = (Map.Entry)itr.next();
          	//System.out.println(pair.getKey() + " = " + pair.getValue());
          	String key = (String)pair.getKey();
          	Integer value = (Integer)pair.getValue();
          	moldQuery += (1.0 + Math.log10(value)) *(Math.log10(numDocs / (Integer)queryIDFMap.get(key)) )*
          				(1.0 + Math.log10(value)) *(Math.log10(numDocs / (Integer)queryIDFMap.get(key)) );
		}
		moldQuery = Math.sqrt(moldQuery);
		//compute mold
		Iterator it = docTermMap.entrySet().iterator();	
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
          	//System.out.println(pair.getKey() + " = " + pair.getValue());
          	String key = (String)pair.getKey();
          	String value = (String)pair.getValue();
          	String[] valueArry = value.split(";");
          	double tfIDFValue = 0.0;
          	//double moldQuery = 0;

          	for(int i = 0; i < valueArry.length; i++){
          		String termTFdocF = valueArry[i];
          		String[] termTFdocFArry = termTFdocF.split(",");
          		double queryTF = 1.0 +  Math.log10( stoppedStemmedWordMap.get(
          			termTFdocFArry[0]) )  ;
          		double queryIDF =  Math.log10( numDocs / 
          						Integer.parseInt(termTFdocFArry[2]) );

          		double queryTFIDF = queryTF * queryIDF;
          		//moldQuery += queryTFIDF;
          		double docTF =  Integer.parseInt(termTFdocFArry[1]);
          		tfIDFValue += queryTFIDF * docTF;

          	}

          	//moldQuery = Math.sqrt(moldQuery);
          	double normLizedTFIDFValue = tfIDFValue / 
          		(moldQuery * Double.parseDouble( docMoldMap.get(key) ) ); 
          	docScoreMap.put(key, normLizedTFIDFValue);
		}

	    Map<String, Double> sortedScoreMap = sortByComparator(docScoreMap);

		Iterator it2 = sortedScoreMap.entrySet().iterator();
		int i = 0;
		File fout = new File("rankResult.txt");
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        while (it2.hasNext()) {
          Map.Entry pair = (Map.Entry)it2.next();
          //System.out.println(pair.getKey() + " = " + pair.getValue());
          bw.write(pair.getKey() + " " + pair.getValue());
          bw.newLine();
          it2.remove(); // avoids a ConcurrentModificationException
           i++;
          if(i == 10) {
          	break;
          }
        }
        bw.close();
 
	}
}