/**
 * Copyright (c) 2009-2015 Scale Unlimited, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scaleunlimited.classify.vectors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public abstract class BaseNormalizer implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseNormalizer.class);

    private static final double MIN_FREQUENCY_REPORT_RATIO = 0.2;

    abstract public void normalize(Vector vector);

    public static void dumpTopTerms(final Vector docFrequencies, List<String> uniqueTerms) {
        int cardinality = docFrequencies.size();
        List<Integer> sortedDocFrequencyIndexes = new ArrayList<Integer>(cardinality);
        for (int i = 0; i < cardinality; i++) {
            sortedDocFrequencyIndexes.add(i);
        }
        
        Collections.sort(sortedDocFrequencyIndexes, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return (int)(docFrequencies.getQuick(o2) - docFrequencies.getQuick(o1));
            }
        });
        
        double maxFrequency = docFrequencies.getQuick(docFrequencies.maxValueIndex());
        StringBuffer topTermsReport = new StringBuffer();
        for (int i = 0; i < cardinality; i++) {
            int index = sortedDocFrequencyIndexes.get(i);
            double frequency = docFrequencies.getQuick(index);
            if ((frequency/maxFrequency) > MIN_FREQUENCY_REPORT_RATIO) {
                topTermsReport.append(String.format("'%s'=%d, ",
                                                    uniqueTerms.get(index),
                                                    (int)frequency));
            }
        }
        
        LOGGER.debug(topTermsReport.toString());
    }

}
