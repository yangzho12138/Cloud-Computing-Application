import java.io.*;
import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MP1 {
    Random generator;
    String userName;
    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    public MP1(String userName) {
        this.userName = userName;
    }


    public Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        long longSeed = Long.parseLong(this.userName);
        this.generator = new Random(longSeed);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public String[] process() throws Exception{
    	String[] topItems = new String[20];
        Integer[] indexes = getIndexes();

    	//TO DO
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        List<String> input = new ArrayList<>();
        String text = null;
        while((text = br.readLine()) != null){
            input.add(text);
        }

        List<String> stopWordsList = Arrays.asList(stopWordsArray);

        List<String> items = new ArrayList<>();

        for(int i = 0; i < indexes.length; i++){
            int index = indexes[i];
            String content = input.get(index).toLowerCase();
            StringTokenizer st = new StringTokenizer(content, delimiters);
            while(st.hasMoreTokens()){
                items.add(st.nextToken().trim());
            }
        }
        // remove all the stop words
        items.removeAll(stopWordsList);

        // get frequencies
        Map<String, Integer> occurTimes = new HashMap<>();
        for(String item: items){
            occurTimes.put(item, occurTimes.getOrDefault(item, 0) + 1);
        }
        // convert map to list to compare frequencies
        List<Map.Entry<String, Integer>> entrys = new ArrayList<>(occurTimes.entrySet());
        Collections.sort(entrys, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                if(o1.getValue() != o2.getValue())
                    return o2.getValue() - o1.getValue();
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        int itemIndex = 0;
        for(Map.Entry<String, Integer> entry: entrys){
            topItems[itemIndex ++] = entry.getKey();
            //System.out.println(entry.getKey()+" "+entry.getValue());
            if(itemIndex >= 20)
                break;
        }

		return topItems;
    }

    public static void main(String args[]) throws Exception {
    	if (args.length < 1){
    		System.out.println("missing the argument");
    	}
    	else{
    		String userName = args[0];
	    	MP1 mp = new MP1(userName);
	    	String[] topItems = mp.process();

	        for (String item: topItems){
	            System.out.println(item);
	        }
	    }
	}

}
