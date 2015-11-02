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

import org.junit.Assert;
import org.junit.Test;

import com.scaleunlimited.classify.LibLinearModel;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.analyzer.StandardAnalyzer;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.TextDatum;
import com.scaleunlimited.classify.vectors.BaseNormalizer;
import com.scaleunlimited.classify.vectors.NullNormalizer;
import com.scaleunlimited.classify.vectors.SetNormalizer;
import com.scaleunlimited.classify.vectors.TfNormalizer;
import com.scaleunlimited.classify.vectors.TfidfNormalizer;
import com.scaleunlimited.classify.vectors.UnitNormalizer;

public class LibLinearModelTest {
    
    private static final String PASSING_PHRASE_1 = "Abracadabra! Arrays are always amazing. ";
    private static final String FAILING_PHRASE_1 = "Bummer! Arrays are only sometimes amazing. ";
    
    // TODO CSc This guy, though different (and with digits ignored) still generates false negatives.
    @SuppressWarnings("unused")
    private static final String FAILING_PHRASE_2 = "Abracadabra! Arrays are sometimes amazing. ";
    
    private static final int NUM_PASSING_DOCS = 10;
    private static final int NUM_FAILING_DOCS = 10;
    
    @Test
    public void testClearModel() {
        IAnalyzer analyzer = new StandardAnalyzer();
        analyzer.reset();
        LibLinearModel filterModel = new LibLinearModel();
        filterModel.reset();

        for (int i = 0; i < NUM_PASSING_DOCS; i++) {
            filterModel.addTrainingTerms(makeTestTerms(analyzer, i, true));
        }
        
        for (int i = 0; i < NUM_FAILING_DOCS; i++) {
            filterModel.addTrainingTerms(makeTestTerms(analyzer, i + NUM_PASSING_DOCS, false));
        }
        
        filterModel.train();
        
        Assert.assertEquals("Passing doc failed filter",
                            "passing",
                            filterModel.classify(makeTestTerms(analyzer, 1000, true)).getLabel());
        Assert.assertEquals("Failing doc passed filter",
                            "failing",
                            filterModel.classify(makeTestTerms(analyzer, 1000, false)).getLabel());
                    
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
        // First create a model with real data.
        IAnalyzer analyzer = new StandardAnalyzer();
        LibLinearModel model1 = new LibLinearModel(clazz);
        model1.reset();
        
        for (int i = 0; i < NUM_PASSING_DOCS; i++) {
            model1.addTrainingTerms(makeTestTerms(analyzer, i, true));
        }
        for (int i = 0; i < NUM_FAILING_DOCS; i++) {
            model1.addTrainingTerms(makeTestTerms(analyzer, i + NUM_PASSING_DOCS, false));
        }
        model1.train();
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        model1.write(out);
        baos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInput in = new DataInputStream(bais);
        LibLinearModel model2 = new LibLinearModel();
        model2.readFields(in);
        
        Assert.assertEquals(model1, model2);
    }

    private TermsDatum makeTestTerms(IAnalyzer analyzer, int index, boolean passing) {
        TermsDatum result = analyzer.getTermsDatum(makeTextDatum(index, passing).getTuple());
        result.setLabel(passing ? "passing" : "failing");
        return result;
    }

    private TextDatum makeTextDatum(int index, boolean passing) {
        TextDatum result =
            new TextDatum(  (   "test user " + index
                            +   " test hashtags " + index
                            +   (   " http://www.test.org/url "
                                +   index
                                +   ".html")
                            +   " test title " + index
                            +   " test description " + index
                            +   " test keywords " + index)
                            +   (   " "
                                +   (passing ?
                                        PASSING_PHRASE_1
                                    :   FAILING_PHRASE_1)
                                +   index));
        return result;
    }
}
