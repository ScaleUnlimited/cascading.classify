/**
 * Copyright (c) 2013-2015 Scale Unlimited, Inc.
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
package com.scaleunlimited.classify;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.scaleunlimited.classify.HashedFeaturesLibLinearModel;
import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.FeaturesDatum;

public class HashedFeaturesLibLinearModelTest {
    
    private static final int NUM_DOCS = 10;
    
    private static final String TRAIN_MAGIC_FEATURES= "Abracadabra\t0.59\tmagic\t0.8\tcaramba\t0.6";
    private static final String TRAIN_MATH_FEATURES= "sum\t0.59\tmultiplication\t0.8\tdivision\t0.6";
    private static final String TRAIN_ELECTROMAGNETIC_FEATURES= "electromagnet\t0.89\tflash\t0.8\tlight\t0.6";
    private static final String TRAIN_MIXED_FEATURES= "Abracadabra\t0.59\tmagic\t0.8\telectromagnet\t0.89";

    private static final String TEST_MAGIC_FEATURES= "magic\t0.8";
    private static final String TEST_MATH_FEATURES= "multiplication\t0.8";

    HashedFeaturesLibLinearModel _model;
    static int _testDocIndex = 0;

    @Before
    public void setUp() throws Exception {
    	// hash down to 10% of original size.
        _model = new HashedFeaturesLibLinearModel(0.1f);
        _model.reset();
    }

    @Test
    public void testModel() throws Exception {
        
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeFeaturesDatum("magic", TRAIN_MAGIC_FEATURES));
        }
        
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeFeaturesDatum("math", TRAIN_MATH_FEATURES));
        }
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeFeaturesDatum("electromagnetic", TRAIN_ELECTROMAGNETIC_FEATURES));
        }
        _model.train();
        
        validateModel(_model);
        
        // Now, to test serialization, save it, read it back in, and verify we get
        // the same results.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        _model.write(out);
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInput in = new DataInputStream(bais);
        HashedFeaturesLibLinearModel newModel = new HashedFeaturesLibLinearModel();
        newModel.readFields(in);
        
        validateModel(newModel);
    }
    
    private void validateModel(HashedFeaturesLibLinearModel model) {
        FeaturesDatum magicDoc = makeFeaturesDatum("magic", TEST_MAGIC_FEATURES);
        FeaturesDatum mathDoc = makeFeaturesDatum("math", TEST_MATH_FEATURES);
        FeaturesDatum emDoc = makeFeaturesDatum("electromagnetic", TRAIN_ELECTROMAGNETIC_FEATURES);
        FeaturesDatum mixedDoc = makeFeaturesDatum("magic", TRAIN_MIXED_FEATURES);

        Assert.assertEquals("Magic doc failed",
                            "magic",
                            model.classify(magicDoc).getLabel());
        Assert.assertEquals("Math doc failed",
                            "math",
                            model.classify(mathDoc).getLabel());
        DocDatum[] nResults = model.classifyNResults(magicDoc, 2);
        Assert.assertEquals(2, nResults.length);
        Assert.assertEquals("Magic doc failed",
                        "magic",
                        nResults[0].getLabel());

        nResults = model.classifyNResults(emDoc, 2);
        Assert.assertEquals(2, nResults.length);
        Assert.assertEquals("Electromagnetic doc failed",
                        "electromagnetic",
                        nResults[0].getLabel());
        
        nResults = model.classifyNResults(mixedDoc, 3);
        Assert.assertEquals(3, nResults.length);
        Assert.assertEquals("Mixed doc failed",
                        "magic",
                        nResults[0].getLabel());
        Assert.assertEquals( "electromagnetic",
                        nResults[1].getLabel());
        
        nResults = model.classifyNResults(mixedDoc, 100);
        Assert.assertEquals(3, nResults.length);
    }
    
    private FeaturesDatum makeFeaturesDatum(String label, String features) {
        Map<String, Double> featureMap = new HashMap<String, Double>();
        String[] split = features.split("\t");
        for (int i = 0; i < split.length; i++) {
            featureMap.put(split[i], Double.parseDouble(split[i+1]));
            i++;
        }
        return new FeaturesDatum(featureMap, label);
    }
    
    @Test
    public void testHashFunction1() {
    	Random rand = new Random(0L);
    	final int moduloValue = 65536;
    	
    	int totalDupStrngs = 0;
    	int totalCollisions = 0;
    	int noCollisions = 0;
    	for (int loop = 0; loop < 1000; loop++) {
    		Set<String> strings = new HashSet<String>();
    		
    		Set<Integer> hashes = new HashSet<Integer>();
    		int collisions = 0;

    		for (int i = 0; i < 200; i++) {
    			String fakeTerm;
    			
    			while (true) {
    				StringBuffer str = new StringBuffer(10);
    				int strLength = 3 + rand.nextInt(8);
    				for (int j = 0; j < strLength; j++) {
    					str.append((char)(0x61 + rand.nextInt(26)));
    				}

    				fakeTerm = str.toString();
    				if (strings.add(fakeTerm)) {
    					break;
    				}
    				
    				totalDupStrngs += 1;
    			}
    			
    			int hashcode = HashedFeaturesLibLinearModel.calcHashJoaat(fakeTerm, moduloValue);
    			System.out.println("Adding hashcode: " + hashcode);
    			if (!hashes.add(hashcode)) {
    				collisions += 1;
    			}
    		}
    		
    		if (collisions == 0) {
    			noCollisions += 1;
    		} else {
    			totalCollisions += collisions;
    		}
    		
    	}
    	
    	System.out.println("Total collisions: " + totalCollisions);
    	System.out.println("No collisions: " + noCollisions);
    	System.out.println("Duplicated strings: " + totalDupStrngs);
    }
   
    @Test
    public void testHashFunction2() {
    	Random rand = new Random(0L);
    	final int moduloValue = 1024;
    	final int expectedCount = 1000;
    	int hashCounts[] = new int[moduloValue];
    	
		Set<String> strings = new HashSet<String>();
		StringBuffer str = new StringBuffer(10);

		for (int i = 0; i < expectedCount*moduloValue; i++) {
			String fakeTerm;
			
			while (true) {
				str.setLength(0);
				
				int strLength = 3 + rand.nextInt(8);
				for (int j = 0; j < strLength; j++) {
					str.append((char)(0x61 + rand.nextInt(26)));
				}

				fakeTerm = str.toString();
				if (strings.add(fakeTerm)) {
					break;
				}
			}
			
			int hashcode = HashedFeaturesLibLinearModel.calcHashJoaat(fakeTerm, moduloValue);
//			int hashcode = rand.nextInt(moduloValue);
			hashCounts[hashcode] += 1;
		}
		
		// Now see how many hash counts are more than 10% different from the expected count.
		int numDeltas[] = new int[100];
		
		
		for (int i = 0; i < moduloValue; i++) {
			int delta = Math.abs(hashCounts[i] - expectedCount);
			int deltaPercent = (int)Math.round(100.0 * delta/(double)expectedCount);
			numDeltas[deltaPercent] += 1;
		}
		
		for (int i = 0; i < 100; i++) {
			if (numDeltas[i] > 0) {
				System.out.println(String.format("%d%% had %d entries", i, numDeltas[i]));
			}
		}
    }
}
