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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;

import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.TermsDatum;

@SuppressWarnings({"serial", "rawtypes"})
public class ClassifyTerms
    extends BaseOperation<NullContext>
    implements Function<NullContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClassifyTerms.class);

    private BaseModel _model;
    private transient Map<String, Integer> _totalClassifications;
    private transient Map<String, Integer> _numBadClassifications;
    private transient LoggingFlowProcess _flowProcess;
    
    ClassifyTerms(BaseModel model) {
        super(DocDatum.FIELDS);
        _model = model;
    }
    
    // Classification is an expensive operation, so we don't want it repeated
    // if our output ends up getting split into two pipes.
    @Override
    public boolean isSafe() {
        return false;
    }

    @SuppressWarnings("unchecked")
	@Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);
        _flowProcess = new LoggingFlowProcess(flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
        _model.reset();
        _totalClassifications = new HashMap<String, Integer>();
        _numBadClassifications = new HashMap<String, Integer>();
    }

    @SuppressWarnings("unchecked")
	@Override
    public void operate(FlowProcess flowProcess,
                        FunctionCall<NullContext> functionCall) {
        TermsDatum termsDatum =
            new TermsDatum(functionCall.getArguments().getTuple());
        DocDatum docDatum = _model.classify(termsDatum);
        docDatum.setPayload(termsDatum.getPayload());
        countClassifications(termsDatum, docDatum);
        functionCall.getOutputCollector().add(docDatum.getTuple());
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        if (LOGGER.isDebugEnabled()) {
            for (Map.Entry<String, Integer> entry : _numBadClassifications.entrySet()) {
                LOGGER.debug(String.format("Number of training documents misclassified as %s: %d",
                                            entry.getKey(),
                                            entry.getValue()));
            }
            for (Map.Entry<String, Integer> entry : _totalClassifications.entrySet()) {
                LOGGER.debug(String.format("Total documents classified as %s: %d",
                                            entry.getKey(),
                                            entry.getValue()));
            }
        }
        _flowProcess.dumpCounters();
        super.cleanup(flowProcess, operationCall);
    }

    private void countClassifications(  TermsDatum termsDatum,
                                        DocDatum docDatum) {
        String termsLabel = termsDatum.getLabel();
        String docLabel = docDatum.getLabel();
        if  (   (termsLabel != null)
            &&  (!(termsLabel.equals(BaseModel.NOT_YET_LABELED)))) {
            if (!(docLabel.equals(termsLabel))) {
                Integer numBad = _numBadClassifications.get(docLabel);
                if (numBad == null) {
                    _numBadClassifications.put(docLabel, 1);
                } else {
                    _numBadClassifications.put(docLabel, ++numBad);
                }
            }
        }
        Integer total = _totalClassifications.get(docLabel);
        if (total == null) {
            _totalClassifications.put(docLabel, 1);
        } else {
            _totalClassifications.put(docLabel, ++total);
        }
        _flowProcess.increment(ClassifyCounters.CLASSIFIED_INPUT_TUPLE, 1);
    }
}