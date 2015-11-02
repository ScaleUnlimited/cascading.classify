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

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.scaleunlimited.classify.vectors.BaseNormalizer;
import com.scaleunlimited.classify.vectors.TfidfNormalizer;


public class TfidfNormalizerTest {

    @Test
    public void testNormalization() {
        List<Vector> vectors = new LinkedList<Vector>();
        
        RandomAccessSparseVector v = new RandomAccessSparseVector(3);
        v.setQuick(0, 2.0);
        v.setQuick(1, 8.0);
        v.setQuick(2, 0.0);
        vectors.add(v);
        
        v = new RandomAccessSparseVector(3);
        v.setQuick(0, 4.0);
        v.setQuick(1, 0.0);
        v.setQuick(2, 0.0);
        vectors.add(v);
        
        BaseNormalizer normalizer = new TfidfNormalizer();
        normalizer.normalize(vectors);
        
        Assert.assertEquals(2, vectors.size());
        
        // FUTURE pass in this similarity to TfIdfNormalizer, so
        // we know for sure what's being used.
        DefaultSimilarity similarity = new DefaultSimilarity();
        
        Vector nv = vectors.get(0);
        double score = similarity.tf(0.2f) * similarity.idf(2, 2);
        Assert.assertEquals(score, nv.get(0), 0.001);
        
        score = similarity.tf(0.8f) * similarity.idf(1, 2);
        Assert.assertEquals(score, nv.get(1), 0.001);
        Assert.assertEquals(0.0, nv.get(2), 0.001);
        
        nv = vectors.get(1);
        score = similarity.tf(1.0f) * similarity.idf(2,  2);
        Assert.assertEquals(score, nv.get(0), 0.001);
        Assert.assertEquals(0.0, nv.get(1), 0.001);
        Assert.assertEquals(0.0, nv.get(2), 0.001);

    }
}
