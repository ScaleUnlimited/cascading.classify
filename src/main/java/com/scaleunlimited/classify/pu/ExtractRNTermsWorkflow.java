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

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.ClassifyDocsPipe;
import com.scaleunlimited.classify.ClassifyOptions;
import com.scaleunlimited.classify.TrainLogisticModelPipe;
import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.ModelDatum;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.ThresholdDatum;

@SuppressWarnings({"serial", "rawtypes"})
public class ExtractRNTermsWorkflow {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractRNTermsWorkflow.class);
    
    public static final int MAX_ITERATIONS = 100;
    
    // If M-Prob(U[i]) < t, then RN.add(U[i])
    public static class FilterRNTerms
        extends BaseOperation<NullContext>
        implements Filter<NullContext> {
        
        private BaseModel _model;
        private double _threshold;
        private transient LoggingFlowProcess _flowProcess;

        public FilterRNTerms(BaseModel model, double threshold) {
            super();
            _model = model;
            _threshold = threshold;
        }

        @SuppressWarnings("unchecked")
		@Override
        public void prepare(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
            _model.reset();
        }

        @SuppressWarnings("unchecked")
		@Override
        public boolean isRemove(FlowProcess flowProcess,
                                FilterCall<NullContext> filterCall) {
            TermsDatum termsDatum =
                new TermsDatum(filterCall.getArguments().getTuple());
            DocDatum docDatum = _model.classify(termsDatum);
            boolean result =
                (TrainLogisticModelPipe.getPositiveScore(docDatum) >= _threshold);
            if (result) {
                _flowProcess.increment(ClassifyPUCounters.RELIABLY_NEGATIVE, 1);
            } else {
                _flowProcess.increment(ClassifyPUCounters.DISCARDED, 1);
            }
            return result;
        }

        @Override
        public void cleanup(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            _flowProcess.dumpCounters();
            super.cleanup(flowProcess, operationCall);
        }
    }

    public static Flow createFlow(BasePlatform platform, ClassifyOptions options)
        throws Exception {
        
        // Find working directory
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        workingDirPath.assertExists("Working directory");
    
        // Read in the model (including analyzer)
        BasePath modelPath = platform.makePath(workingDirPath, ClassifyPUConfig.SPY_MODEL_SUBDIR_NAME);
        ModelDatum modelDatum =
            ClassifyDocsPipe.readModel(platform, modelPath);
        
        // Read in the reliably negative threshold
        BasePath thresholdPath = platform.makePath(workingDirPath, ClassifyPUConfig.RELIABLY_NEGATIVE_THRESHOLD_SUBDIR_NAME);
        ThresholdDatum thresholdDatum =
            readThreshold(platform, thresholdPath);
        
        // Set up the input source
        // Note that we'll either refine the existing reliably negative set or
        // build a new one from the unlabeled set. This means that you've got to
        // clean out any leftover reliably negative subdirs before the first time
        // we're used new training data (as AnalyzeTrainingDataWorkflow does).
        BasePath unlabeledPath = platform.makePath(workingDirPath, ClassifyPUConfig.UNLABELED_TERMS_SUBDIR_NAME);
        BasePath reliablyNegativePath = platform.makePath(workingDirPath, ClassifyPUConfig.RELIABLY_NEGATIVE_TERMS_SUBDIR_NAME);
        if (reliablyNegativePath.exists()) {
            LOGGER.info(String.format(  "Refining existing reliably negative training terms in %s",
                                        unlabeledPath));
            BasePath previousReliablyNegativePath =
                getPreviousReliablyNegativePath(platform, workingDirPath);
            
            platform.rename(reliablyNegativePath, previousReliablyNegativePath);
            unlabeledPath = previousReliablyNegativePath;
        } else {
            unlabeledPath.assertExists("Unlabeled training directory");
        }
        Tap unlabeledSource = platform.makeTap(  platform.makeBinaryScheme(TermsDatum.FIELDS),
                                        unlabeledPath);
        
        // If M-Prob(U[i]) < t, then RN.add(U[i])
        // Note that training data has been pre-analyzed.
        Pipe unlabeledPipe = new Pipe("unlabeled");
        Pipe reliablyNegativePipe = new Pipe("reliably negative", unlabeledPipe);
        reliablyNegativePipe =
            new Each(   reliablyNegativePipe,
                        new FilterRNTerms(  modelDatum.getModel(),
                                            thresholdDatum.getThreshold()));
        reliablyNegativePipe = TupleLogger.makePipe(reliablyNegativePipe, true);
    
        // Set up the output sink
        Tap reliablyNegativeSink = platform.makeTap( platform.makeBinaryScheme(TermsDatum.FIELDS),
                                            reliablyNegativePath,
                                            SinkMode.REPLACE);
        
        // Build and return the workflow
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(   unlabeledSource,
                                        reliablyNegativeSink,
                                        reliablyNegativePipe);
    }
    
    // If "...-000" exists, we return 2
    public static int getNextIterationIndex(    BasePlatform platform,
                                                BasePath workingDirPath) throws Exception {
        BasePath reliablyNegativePath =
            platform.makePath(workingDirPath, ClassifyPUConfig.RELIABLY_NEGATIVE_TERMS_SUBDIR_NAME);
        if (!(reliablyNegativePath.exists())) {
            return 0;
        }
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            BasePath prevPath = makePreviousReliablyNegativePath(platform, reliablyNegativePath, i);
            if (!(prevPath.exists())) {
                return (i+1);
            }
        }
        throw new RuntimeException("Too many iterations!");
    }

    public static BasePath makePreviousReliablyNegativePath(BasePlatform platform, BasePath reliablyNegativePath,
                                                        int index) throws Exception {
        return platform.makePath(String.format( "%s-%03d",
                        reliablyNegativePath.getAbsolutePath(),
                        index));
    }

    // If this is iteration 2, we return "...=001"
    private static BasePath getPreviousReliablyNegativePath(BasePlatform platform,
                                                        BasePath workingDirPath)
        throws Exception {
        
        return makePreviousReliablyNegativePath( platform, platform.makePath(   workingDirPath,
                                                            ClassifyPUConfig.RELIABLY_NEGATIVE_TERMS_SUBDIR_NAME),
                                                getNextIterationIndex(  platform,
                                                                        workingDirPath)-1);
    }

    @SuppressWarnings("unchecked")
	public static ThresholdDatum readThreshold( BasePlatform platform,
                                                BasePath thresholdPath)
        throws Exception {
        
        thresholdPath.assertExists("Threshold directory");
        Tap thresholdSource = platform.makeTap(  platform.makeBinaryScheme(ThresholdDatum.FIELDS),
                                        thresholdPath);
        Iterator<TupleEntry> iter = thresholdSource.openForRead(platform.makeFlowProcess());
        if (!(iter.hasNext())) {
            throw new IllegalStateException(String.format(  "Threshold directory %s doesn't contain any classifiers",
                                                            thresholdPath));
        }
        ThresholdDatum result = new ThresholdDatum(iter.next().getTuple());
        if (iter.hasNext()) {
            throw new IllegalStateException(String.format(  "Threshold directory %s has more than one classifier",
                                                            thresholdPath));
        }
        return result;
    }
}
