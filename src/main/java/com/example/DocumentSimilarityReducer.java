package com.example;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;


/**
 * Reducer for the document similarity job.
 *
 * Input:  whatever your mapper emits, grouped by key by the shuffle/sort phase.
 *
 * Output: one line per pair of documents that share at least one word, in exactly this
 *         format (see README.md):
 *
 *             Doc01, Doc02 Similarity: 0.18
 *
 *         where the two IDs are in ascending String order (Doc01 before Doc02), and the
 *         Jaccard similarity  |A ∩ B| / |A ∪ B|  is printed with two decimals, e.g.
 *         String.format("%.2f", similarity). Note that "%.2f" uses the machine's locale;
 *         use  String.format(java.util.Locale.US, "%.2f", similarity)  to be safe.
 *
 * Hint: in the design suggested in README.md all documents reach a single reducer, one per
 *       reduce() call. You cannot compare documents until you have seen all of them, so
 *       reduce() only stores each document, and the pairwise comparison happens in
 *       cleanup(), which Hadoop calls once after the last reduce() call.
 */
public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, NullWritable> {

    private Map<String, Set<String>> documents = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String documentId = key.toString();
        Set<String> tokens = new HashSet<>();

        for (Text value : values) {
            String[] words = value.toString().split("\\s+");
            for (String string : words) {
                tokens.add(string);
            }
            
        }
        
        documents.put(documentId, tokens);


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<String> documentIds = new ArrayList<>(documents.keySet());
        documentIds.sort(String::compareTo);
        for (int i = 0; i <documentIds.size(); i++){
            for (int j = i+1; j< documentIds.size(); j++){
                String docA = documentIds.get(i);
                String docB = documentIds.get(j);
                Set<String> docAWords = documents.get(docA);
                Set<String> docBWords = documents.get(docB);

                Set<String> intersection = new HashSet<>(docAWords);
                intersection.retainAll(docBWords);
                int intersectionSize = intersection.size();
                
                Set<String> union = new HashSet<>(docAWords);
                union.addAll(docBWords);
                int unionSize = union.size();

                double SimilarityScore = (double) intersectionSize/unionSize;
                
                if (intersectionSize >0){
                    String output = docA + ", " + docB + " Similarity: " + String.format(java.util.Locale.US, "%.2f", SimilarityScore);
                    context.write(new Text(output), NullWritable.get());
                }
            }
        }

    }
}
