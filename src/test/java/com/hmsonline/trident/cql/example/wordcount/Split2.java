package com.hmsonline.trident.cql.example.wordcount;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by bishao on 1/15/15.
 */
public class Split2 extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        for(String word: tuple.getString(0).split(" ")) {
            if(word.length() > 0) {
                if (word.contains(",")) {
                    String[] str = word.split(",");
                    collector.emit(new Values(str[0], str[1]));
                } else {
                    collector.emit(new Values(word));
                }

            }
        }
    }

}
