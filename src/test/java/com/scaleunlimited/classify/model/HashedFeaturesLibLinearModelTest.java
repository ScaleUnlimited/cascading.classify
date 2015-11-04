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
package com.scaleunlimited.classify.model;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import com.scaleunlimited.classify.model.BaseLibLinearModel;
import com.scaleunlimited.classify.model.HashedFeaturesLibLinearModel;

public class HashedFeaturesLibLinearModelTest extends BaseLibLinearModelTest {
    
    @Override
    protected BaseLibLinearModel getModel() {
    	return new HashedFeaturesLibLinearModel().setPercentReduction(0.10f);
    };

    @Test
    public void testModel() throws Exception {
    	super.testModel();
    }
    
    @Test
    public void testGettingDetails() throws Exception {
    	super.testGettingDetails();
    }
    
    @Test
    public void testSerializationWithAllNormalizers() throws Exception {
    	super.testSerializationWithAllNormalizers();
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
