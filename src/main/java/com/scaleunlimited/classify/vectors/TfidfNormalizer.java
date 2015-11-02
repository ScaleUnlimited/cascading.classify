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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.function.DoubleDoubleFunction;

@SuppressWarnings("serial")
public class TfidfNormalizer extends BaseNormalizer {
    private DefaultSimilarity _similarity;
    
    public TfidfNormalizer() {
        _similarity = new DefaultSimilarity();
    }
    
    @Override
    public void normalize(List<? extends Vector> vectors) {
        normalize(vectors, null);
    }

    public void normalize(  List<? extends Vector> vectors,
                            List<String> uniqueTerms) {
        if (vectors.size() == 0) {
            throw new IllegalArgumentException("Can't normalize empty list of vectors");
        }
        
        int cardinality = vectors.get(0).size();
        
        // First generate document counts for each term.
        final DenseVector docFrequencies = new DenseVector(cardinality);
        for (Vector v : vectors) {
            if (v.size() != cardinality) {
                throw new IllegalArgumentException("All vectors must have the same cardinality: " + cardinality);
            }
            
            docFrequencies.assign(v, new DoubleDoubleFunction() {

                @Override
                public double apply(double docCount, double vectorValue) {
                    return docCount + (vectorValue > 0 ? 1.0 : 0.0);
                }
            });
        }
        
        if (uniqueTerms != null) {
            dumpTopTerms(docFrequencies, uniqueTerms);
        }
        
        final long numDocs = vectors.size();
        
        // Now, for each vector we want to normalize by total count, and
        // then divide by document frequency.
        for (int i = 0; i < numDocs; i++) {
            normalize(vectors.get(i), docFrequencies, numDocs);
        }
    }
    
    public void normalize(Vector vector, final Vector docFrequencies, long numDocs) {
        double totalCount = vector.zSum();
        
        for (Element e : vector.nonZeroes()) {
            float freq = (float)(e.get() / totalCount);
            float score = _similarity.tf(freq) * _similarity.idf((long)docFrequencies.getQuick(e.index()), numDocs);
            vector.setQuick(e.index(), score);
        }
    }
    
    /* (non-Javadoc)
     * @see com.scaleunlimited.classify.vectors.BaseNormalizer#normalize(org.apache.mahout.math.Vector)
     * 
     * When we're called to normalize a single vector, this is for classification. In this situation we
     * don't have to calculate IDF, so assume it's in one document.
     */
    @Override
    public void normalize(Vector vector) {
        double totalCount = vector.zSum();

        for (Element e : vector.nonZeroes()) {
            float freq = (float)(e.get() / totalCount);
            float score = _similarity.tf(freq) * _similarity.idf(1, 1);
            vector.setQuick(e.index(), score);
        }
    }

}
