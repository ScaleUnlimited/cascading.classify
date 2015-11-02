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
package com.scaleunlimited.classify.pu;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.TextDatum;

public class AnalyzeTrainingDataPipeTest extends AbstractWorkflowTest {

    private static final String[] ANIMALS = {   "badger",
                                                "cat",
                                                "mongoose",
                                                "hyena",
    };
    private static final String PAYLOAD_KEY = "test-payload";

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }
    
    @Test
    public void createFlow() throws Exception {
        makeTestData(_positivePath, NUM_POSITIVE_DATUMS, true);
        makeTestData(_unlabeledPath, NUM_UNLABELED_DATUMS, false);
        AnalyzeTrainingDataOptions options = new AnalyzeTrainingDataOptions();
        options.setDebugLogging(false);
        options.setWorkingDir(WORKING_DIR);
        options.setAnalyzerName("Standard");
        Flow flow = AnalyzeTrainingDataPipe.createTextFlow(_platform,options);
        flow.complete();
        this.checkSinkDirsExist();
        checkRNDirsDeleted();
        checkTerms(_positiveTermsPath, true);
        checkTerms(_unlabeledTermsPath, false);
    }

    public boolean sinkDirsExist() throws IOException {
        return(_positiveTermsPath.exists() &&  _unlabeledTermsPath.exists());
    }
    
    public void checkSinkDirsExist() throws IOException {
        _positiveTermsPath.assertExists("Positive terms directory");
        _unlabeledTermsPath.assertExists("Unlabeled terms directory");
    }

    private void checkRNDirsDeleted() throws Exception {
        String errorMessage =
            String.format(  "Reliably negative terms path not deleted: %s",
                            _reliablyNegativeTermsPath);
        Assert.assertFalse( errorMessage,
                        _reliablyNegativeTermsPath.exists());
        for (int i = 0; i < ExtractRNTermsWorkflow.MAX_ITERATIONS; i++) {
            BasePath previousReliablyNegativePath =
                ExtractRNTermsWorkflow.makePreviousReliablyNegativePath(_platform, _reliablyNegativeTermsPath, i);
            errorMessage =
                String.format(  "Previous reliably negative terms path not deleted: %s",
                                previousReliablyNegativePath);
            Assert.assertFalse( errorMessage, previousReliablyNegativePath.exists());
        }
        
    }

    private void checkTerms(BasePath termsPath, boolean isPositive)
        throws Exception {
        
        Iterator<TupleEntry> iter = openTermsSource(termsPath);
        Assert.assertTrue(  "No terms found in path " + termsPath.toString(),
                            iter.hasNext());
        while (iter.hasNext()) {
            TermsDatum termsDatum = new TermsDatum(iter.next().getTuple());
            String payloadString =
                (String)(termsDatum.getPayloadValue(PAYLOAD_KEY));
            Assert.assertNotNull("missing payload", payloadString);
            Integer.parseInt(payloadString);
            Assert.assertTrue(  "unexpected payload content",
                                termsDatum.getTermMap().containsKey(payloadString));
            String errorMessage =
                String.format(  "Unexpected frequency of term '%s' in TermsDatum '%s'",
                                DOUBLE_FREQUENCY_TERM,
                                termsDatum.getTuple());
            Assert.assertEquals(errorMessage,
                                Integer.valueOf(2),
                                termsDatum.getTermMap().get(DOUBLE_FREQUENCY_TERM));
            if (isPositive) {
                checkPositiveTerm(termsDatum);
            }
        }
    }

    private void checkPositiveTerm(TermsDatum termsDatum) {
        String errorMessage =
            String.format(  "positive TermsDatum doesn't contain '%s': '%s'",
                            POSITIVE_TERM,
                            termsDatum.getTuple());
        Assert.assertTrue(  errorMessage,
                            termsDatum.getTermMap().containsKey(POSITIVE_TERM));
    }

    private void makeTestData(  BasePath outputPath,
                                int numDatums,
                                boolean forceAllPositive)
        throws Exception {
        
        Tap sink = _platform.makeTap( _platform.makeBinaryScheme(TextDatum.FIELDS),
                            outputPath,
                            SinkMode.REPLACE);
        TupleEntryCollector outputCollector = sink.openForWrite(_platform.makeFlowProcess());
        for (int i = 0; i < numDatums; i++) {
            TextDatum textDatum = makeTextDatum(i, forceAllPositive);
            outputCollector.add(textDatum.getTuple());
        }
        outputCollector.close();
    }

    private TextDatum makeTextDatum(int index, boolean forcePositive) {
        StringBuffer content = new StringBuffer();
        content.append(String.format("Test datum %d", index));
        if  (   (forcePositive)
            ||  (_random.nextBoolean())) {
            content.append(String.format(" is %s, but", POSITIVE_TERM));
        }
        content.append(" might as well be the");
        content.append(" " + DOUBLE_FREQUENCY_TERM);
        content.append(" entrails of a");
        content.append(" " + DOUBLE_FREQUENCY_TERM);
        content.append(" " + ANIMALS[_random.nextInt(ANIMALS.length)]);
        TextDatum result = new TextDatum(content.toString());
        result.setPayloadValue(PAYLOAD_KEY, String.valueOf(index));
        return result;
    }

}
