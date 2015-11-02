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

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

public class TfNormalizerTest {

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

        BaseNormalizer normalizer = new TfNormalizer();
        normalizer.normalize(vectors);

        Assert.assertEquals(2, vectors.size());
        Vector v1 = vectors.get(0);
        assertEquals(0.2, v1.get(0), 0.001);
        assertEquals(0.8, v1.get(1), 0.001);
        assertEquals(0.0, v1.get(2), 0.001);
        
        Vector v2 = vectors.get(1);
        assertEquals(1.0, v2.get(0), 0.001);
        assertEquals(0.0, v2.get(1), 0.001);
        assertEquals(0.0, v2.get(2), 0.001);
    }

}
