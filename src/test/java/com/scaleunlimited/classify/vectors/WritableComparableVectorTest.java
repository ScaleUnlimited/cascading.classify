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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.Assert;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.scaleunlimited.classify.vectors.WritableComparableVector;

public class WritableComparableVectorTest {

    @Test
    public void testToFromStream() throws Exception {
        WritableComparableVector vector1 =
            new WritableComparableVector(makeVector());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        vector1.write(dos);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream dis = new DataInputStream(bais);
        WritableComparableVector vector2 = new WritableComparableVector(null);
        vector2.readFields(dis);
        compareVectors(vector1.getVector(), vector2.getVector());
    }

    private Vector makeVector() {
        Vector vector = new RandomAccessSparseVector(2);
        vector.setQuick(0, 5);
        vector.setQuick(1, 10.0);
        return vector;
    }
    
    private void compareVectors(Vector vector1, Vector vector2) {
        Assert.assertEquals(vector1.size(), vector2.size());
        for (int i = 0; i < vector1.size(); i++) {
            Assert.assertEquals(vector1.getQuick(i), vector2.getQuick(i));
        }
    }

}
