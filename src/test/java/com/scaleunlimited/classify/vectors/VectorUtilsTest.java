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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.Assert;
import org.junit.Test;

import com.scaleunlimited.classify.vectors.VectorUtils;


public class VectorUtilsTest {

    @Test
    public void testMakeExtraVector() {
        List<String> uniqueTerms = new ArrayList<String>(2);
        uniqueTerms.add("a");
        uniqueTerms.add("b");
        
        Map<String, Integer> docTerms = new HashMap<String, Integer>();
        docTerms.put("a", 1);
        docTerms.put("c", 5);
        
        Vector v = VectorUtils.makeExtraVector(uniqueTerms, docTerms);
        Assert.assertEquals(1, v.size());
        Assert.assertEquals(5, new Double(v.get(0)).intValue());
    }
    
    @Test
    public void testExtendVector() {
        List<String> uniqueTerms = new ArrayList<String>(2);
        uniqueTerms.add("a");
        uniqueTerms.add("b");

        Vector v1 = VectorUtils.makeVector(uniqueTerms, uniqueTerms);
        Vector v2 = VectorUtils.extendVector(v1, 1);
        
        Assert.assertEquals(1, new Double(v2.get(0)).intValue());
        Assert.assertEquals(1, new Double(v2.get(1)).intValue());
        Assert.assertEquals(0, new Double(v2.get(2)).intValue());
    }
    
    @Test
    public void testAppend() {
        Vector v1 = new RandomAccessSparseVector(2);
        v1.setQuick(0, 0);
        v1.setQuick(1, 1);
        
        Vector v2 = new RandomAccessSparseVector(3);
        v2.setQuick(0, 2);
        v2.setQuick(1, 3);
        v2.setQuick(2, 4);
        
        Vector v3 = VectorUtils.appendVectors(v1, v2);
        
        Assert.assertEquals(5, v3.size());
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(i, new Double(v3.getQuick(i)).intValue());
        }
    }
}
