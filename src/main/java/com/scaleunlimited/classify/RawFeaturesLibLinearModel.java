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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.FeaturesDatum;
import com.scaleunlimited.classify.vectors.VectorUtils;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;
import de.bwaldvogel.liblinear.Train;

@SuppressWarnings("serial")
public class RawFeaturesLibLinearModel extends BaseModel<FeaturesDatum> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RawFeaturesLibLinearModel.class);

    private static final SolverType DEFAULT_SOLVER_TYPE = SolverType.L2R_LR;
    private static final double DEFAULT_C = 10;
    private static final double DEFAULT_EPS = 0.01;

    private static final int DEFAULT_NR_FOLD = 5;   // Used when cross validating

    
    private SolverType _solverType = DEFAULT_SOLVER_TYPE;
    private double _constraintsViolation = DEFAULT_C;
    private double _eps = DEFAULT_EPS;

    private List<String> _labelNames;
    private List<String> _uniqueTerms;
    private int _modelLabelIndexes[] = null;

    private transient List<String> _labelList;
    private transient List<Map<String, Double>> _featuresList;
    private transient Model _model;

    private boolean _crossValidationRequired = true;
    private boolean _quietMode = false;
    
    @Override
    public void reset() {
        if (_labelList == null) {
            _labelList = new ArrayList<String>();
        } else {
            _labelList.clear();
        }
        
        if (_featuresList == null) {
            _featuresList = new ArrayList<Map<String, Double>>();
        } else {
            _featuresList.clear();
        }
    }

    @Override
    public void addTrainingTerms(FeaturesDatum featuresDatum) {
        _model = null;
        _labelList.add(featuresDatum.getLabel());
        _featuresList.add(featuresDatum.getFeatureMap());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        _labelNames = readStrings(in);
        _uniqueTerms = readStrings(in);
        _model = Linear.loadModel(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        LOGGER.info("Saving model...");
        writeStrings(out, _labelNames);
        writeStrings(out, _uniqueTerms);
        
        // Make sure we've got a model to write out.
        if (_model == null) {
            train();
        }
        
        Linear.saveModel(out, _model);
        LOGGER.info("Model saved successfully");
    }

    public void train() {
        _uniqueTerms = buildUniqueTerms(_featuresList);
        List<Vector> vectors = new ArrayList<Vector>(_featuresList.size());
        for (Map<String, Double> featureMap : _featuresList) {
            vectors.add(VectorUtils.makeVectorDouble(_uniqueTerms, featureMap));
        }
        
        _labelNames = new ArrayList<String>();
        for (String label : _labelList) {
            if (!(_labelNames.contains(label))) {
                _labelNames.add(label);
            }
        }
        Collections.sort(_labelNames);
        List<Double> labelIndexList = new ArrayList<Double>(_labelNames.size());
        for (String label : _labelList) {
            int index = Collections.binarySearch(_labelNames, label);
            if (index >= 0) {
                labelIndexList.add((double)index);
            } else {
                throw new RuntimeException("Index not found for label :" + label);
            }
        }
        
        _featuresList.clear();
        
        List<Feature[]> featureList = getFeaturesList(vectors);
        int vectorsSize = vectors.get(0).size();
        vectors.clear();
        
        if (_quietMode) {
            Linear.disableDebugOutput();
        }
        LOGGER.info("Constructing problem for training...");
        Problem problem = Train.constructProblem(   labelIndexList,
                                                    featureList,
                                                    vectorsSize,
                                                    -1.0);
        Parameter param = createParameter();
        LOGGER.info("Starting training...");
        _model = Linear.train(problem, param);
        LOGGER.info(String.format("Trained model with %d classes and %d features", _model.getNrClass(), _model.getNrFeature()));

        if (_crossValidationRequired) {
            double[] target = new double[problem.l];
            LOGGER.info("Cross validating...");
            Linear.crossValidation(problem, param, DEFAULT_NR_FOLD, target);
            int totalCorrect = 0;
            for (int i = 0; i < problem.l; i++) {
                if (target[i] == problem.y[i]) {
                    ++totalCorrect;
                }
            }
            
            LOGGER.info(String.format("Correct: %d%n", totalCorrect));
            LOGGER.info(String.format("Cross Validation Accuracy = %g%%%n", 100.0 * totalCorrect / problem.l));
        }
    }
    
    public DocDatum classify(FeaturesDatum datum) {
        
        Vector docVector = VectorUtils.makeVectorDouble(_uniqueTerms, datum.getFeatureMap());
        FeatureNode[] features = vectorToFeatureNodes(docVector);
        double[] probEstimates = new double[_labelNames.size()];
        
        int labelIndex = (int)Linear.predictProbability( _model,
                                                    features,
                                                    probEstimates);
        String labelName = _labelNames.get(labelIndex);
        
        if (_modelLabelIndexes == null) {
            _modelLabelIndexes = _model.getLabels();
        }
        float score = 0;
        // TODO CSc This could be made more efficient than a linear search
        for (int i = 0; i < _modelLabelIndexes.length; i++) {
            if (_modelLabelIndexes[i] == labelIndex) {
                score = (float)(probEstimates[i]);
            }
        }
        return new DocDatum(labelName, score);
    }
    
    public DocDatum[] classifyNResults(FeaturesDatum datum, int n) {
        Vector docVector = VectorUtils.makeVectorDouble(_uniqueTerms, datum.getFeatureMap());
        FeatureNode[] features = vectorToFeatureNodes(docVector);
        double[] probEstimates = new double[_labelNames.size()];
        
//        int topScoreIndex = 
            Linear.predictProbability( _model,
                        features,
                        probEstimates);
//        String labelName = _labelNames.get(topScoreIndex);
 
        if (_modelLabelIndexes == null) {
            _modelLabelIndexes = _model.getLabels();
        }
 
        SortedSet<LabelIndexScore> labelIndexScoreSet = new TreeSet<LabelIndexScore>();
        double lowestScore = 0.0 ;
        for (int i = 0; i < probEstimates.length; i++) {
            double score = probEstimates[i];

            if (labelIndexScoreSet.size() >= n) {
                if (score > lowestScore) {
                    LabelIndexScore indexScore = new LabelIndexScore(_modelLabelIndexes[i], score);
                    LabelIndexScore first = labelIndexScoreSet.first();
                    labelIndexScoreSet.remove(first);
                    labelIndexScoreSet.add(indexScore);
                    // And now get the new lowest score
                    lowestScore = labelIndexScoreSet.first().getScore();
                }
            } else {
                LabelIndexScore indexScore = new LabelIndexScore(_modelLabelIndexes[i], score);
                labelIndexScoreSet.add(indexScore);
                lowestScore = labelIndexScoreSet.first().getScore();
            }
        }
        
        
        int size = labelIndexScoreSet.size();
        DocDatum[] docDatums = new DocDatum[size];
        
        // Get the top terms from highest to lowest.
        int i = size - 1 ;
        Iterator<LabelIndexScore> iter = labelIndexScoreSet.iterator();
        while (iter.hasNext() && i >= 0) {
            LabelIndexScore next = iter.next();
            docDatums[i] = new DocDatum(_labelNames.get(next.getLabelIndex()), (float)next.getScore());
            i--;
        }
        
//        assert(_labelNames.get(topScoreIndex).equals(docDatums[0].getLabel()));
        return docDatums;
    }
    
    
    public void setQuietMode(boolean quietMode) {
        _quietMode  = quietMode;
    }

    public void setCrossValidation(boolean required) {
        _crossValidationRequired  = required;
    }
    
    public void setMultiClassSolverType(boolean multiClassSolverType) {
        if (multiClassSolverType) {
            _solverType = SolverType.MCSVM_CS;
            _eps = 0.1;
        }
    }
    
    public void setC(double c) {
        _constraintsViolation = c;
    }

    public void setEPS(double eps) {
        _eps = eps;
    }

    private Parameter createParameter() {
        return new Parameter(_solverType, _constraintsViolation, _eps);
    }
    
    private List<Feature[]> getFeaturesList(List<Vector> vectors) {
        List<Feature[]> result = new ArrayList<Feature[]>();
        for (Vector vector : vectors) {
            FeatureNode[] x = vectorToFeatureNodes(vector);
            result.add(x);
        }
        return result;
    }

    private FeatureNode[] vectorToFeatureNodes(Vector vector) {
        int featureCount = vector.getNumNondefaultElements();
        FeatureNode[] x = new FeatureNode[featureCount];
        int arrayIndex = 0;
        int cardinality = vector.size();
        for (int i = 0; i < cardinality; i++) {
            double value = vector.getQuick(i);
            if (value != 0.0) {
                // (At least) Linear.train assumes that FeatureNode.index
                // is 1-based, and we don't really have to map back to our
                // term indexes, so just add one. YUCK!
                x[arrayIndex++] = new FeatureNode(i+1, value);
            }
        }
        return x;
    }
    
    private List<String> buildUniqueTerms(List<Map<String, Double>> featuresList) {
        Set<String> uniqueTerms = new HashSet<String>();
        for (Map<String, Double> termMap : featuresList) {
            uniqueTerms.addAll(termMap.keySet());
        }
        List<String> sortedTerms = new ArrayList<String>(uniqueTerms);
        Collections.sort(sortedTerms);
        return sortedTerms;
    }

    private class LabelIndexScore implements Comparable<LabelIndexScore> {
    
        private int _labelIndex;
        private double _score;
        
        public LabelIndexScore(int labelIndex, double score) {
            _labelIndex = labelIndex;
            _score = score;
        }
    
        public int getLabelIndex() {
            return _labelIndex;
        }
    
        public double getScore() {
            return _score;
        }
    
        @Override
        public int compareTo(LabelIndexScore a) {
            if (_score < a.getScore()) {
                return -1;
            } else if (_score == a.getScore()) {
                return 0;
            } else {
                return 1;
            }
        }
    }

}
