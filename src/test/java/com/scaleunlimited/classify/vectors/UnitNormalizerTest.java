/**
 * Copyright (c) 2015 Scale Unlimited, Inc.
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

import static org.junit.Assert.assertEquals;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.junit.Test;

public class UnitNormalizerTest {

    @Test
    public void testNormalization() {
        BaseNormalizer normalizer = new UnitNormalizer();

        RandomAccessSparseVector v = new RandomAccessSparseVector(3);
        v.setQuick(0, 2.0);
        v.setQuick(1, 8.0);
        v.setQuick(2, 0.0);
        normalizer.normalize(v);
        assertEquals(1.0, v.getLengthSquared(), 0.001);

        v = new RandomAccessSparseVector(3);
        v.setQuick(0, 4.0);
        v.setQuick(1, 0.0);
        v.setQuick(2, 0.0);
        normalizer.normalize(v);

        assertEquals(1.0, v.getLengthSquared(), 0.001);
    }

}
