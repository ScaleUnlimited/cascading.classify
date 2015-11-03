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
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.analyzer.StandardAnalyzer;
import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.FeaturesDatum;
import com.scaleunlimited.classify.vectors.BaseNormalizer;
import com.scaleunlimited.classify.vectors.NullNormalizer;
import com.scaleunlimited.classify.vectors.SetNormalizer;
import com.scaleunlimited.classify.vectors.TfNormalizer;
import com.scaleunlimited.classify.vectors.TfidfNormalizer;
import com.scaleunlimited.classify.vectors.UnitNormalizer;

public class RawFeaturesLibLinearModelTest {
    
    private static final int NUM_DOCS = 10;
    
    private static final String TRAIN_MAGIC_FEATURES= "Abracadabra\t0.59\tmagic\t0.8\tcaramba\t0.6";
    private static final String TRAIN_MATH_FEATURES= "sum\t0.59\tmultiplication\t0.8\tdivision\t0.6";
    private static final String TRAIN_ELECTROMAGNETIC_FEATURES= "electromagnet\t0.89\tflash\t0.8\tlight\t0.6";
    private static final String TRAIN_MIXED_FEATURES= "Abracadabra\t0.59\tmagic\t0.8\telectromagnet\t0.89";

    private static final String TEST_MAGIC_FEATURES= "magic\t0.8";
    private static final String TEST_MATH_FEATURES= "multiplication\t0.8";

    RawFeaturesLibLinearModel _model;

    @Before
    public void setUp() throws Exception {
        _model = new RawFeaturesLibLinearModel();
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
        RawFeaturesLibLinearModel newModel = new RawFeaturesLibLinearModel();
        newModel.readFields(in);
        
        validateModel(newModel);
    }
    
    private void validateModel(RawFeaturesLibLinearModel model) {
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
    
    @Test
    public void testGettingDetails() throws Exception {
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

        System.out.println(_model.getDetails());
    }
    
    @Test
    public void testSerializationWithAllNormalizers() throws Exception {
        testSerialization(NullNormalizer.class);
        testSerialization(UnitNormalizer.class);
        testSerialization(SetNormalizer.class);
        testSerialization(TfNormalizer.class);
        testSerialization(TfidfNormalizer.class);
    }
    
    private void testSerialization(Class<? extends BaseNormalizer> clazz) throws Exception {
    	// TODO use normalizer class when constructing model
        RawFeaturesLibLinearModel model1 = new RawFeaturesLibLinearModel();
        model1.reset();
        
        model1.addTrainingTerms(makeFeaturesDatumFromText("good", "This is an example of some good text"));
        model1.addTrainingTerms(makeFeaturesDatumFromText("bad", "This has lots of bad words in it"));
        
        model1.train();
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        model1.write(out);
        baos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInput in = new DataInputStream(bais);
        RawFeaturesLibLinearModel model2 = new RawFeaturesLibLinearModel();
        model2.readFields(in);
        
        Assert.assertEquals(model1, model2);
    }


    private FeaturesDatum makeFeaturesDatumFromText(String label, String text) {
    	Set<String> terms = new HashSet<>();
    	for (String term : text.split(" ")) {
    		terms.add(term);
    	}
    	
    	double unitValue = Math.sqrt(terms.size());
    	Map<String, Double> featureMap = new HashMap<>();
    	for (String term : terms) {
    		featureMap.put(term, unitValue);
    	}
    	
		return new FeaturesDatum(featureMap, label);
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
   
    
}
