package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {

    private final Map<String, Set<String>> documents = new HashMap<>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String docId = key.toString();
        Set<String> words = new HashSet<>();

        for (Text v : values) {
            words.add(v.toString());
        }

        documents.put(docId, words);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<String> docIds = new ArrayList<>(documents.keySet());
        Collections.sort(docIds);

        for (int i = 0; i < docIds.size(); i++) {
            for (int j = i + 1; j < docIds.size(); j++) {
                String docA = docIds.get(i);
                String docB = docIds.get(j);

                Set<String> setA = documents.get(docA);
                Set<String> setB = documents.get(docB);

                Set<String> intersection = new HashSet<>(setA);
                intersection.retainAll(setB);

                Set<String> union = new HashSet<>(setA);
                union.addAll(setB);

                double similarity = union.isEmpty() ? 0.0 :
                        (double) intersection.size() / union.size();

                String simStr = String.format("%.2f", similarity);

                context.write(
                        new Text(docA + ", " + docB),
                        new Text("Similarity: " + simStr)
                );
            }
        }
    }
}