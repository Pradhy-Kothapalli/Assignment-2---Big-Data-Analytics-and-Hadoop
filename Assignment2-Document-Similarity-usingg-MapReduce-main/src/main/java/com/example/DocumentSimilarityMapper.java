package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outVal = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] tokens = line.split("\\s+");
        if (tokens.length < 2) return;

        String docId = tokens[0];
        outKey.set(docId);

        Set<String> uniqueWords = new HashSet<>();

        for (int i = 1; i < tokens.length; i++) {
            String w = tokens[i].toLowerCase().replaceAll("[^a-z0-9]", "");
            if (!w.isEmpty()) uniqueWords.add(w);
        }

        for (String w : uniqueWords) {
            outVal.set(w);
            context.write(outKey, outVal);
        }
    }
}