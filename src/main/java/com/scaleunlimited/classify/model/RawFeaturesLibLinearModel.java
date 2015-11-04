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
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.vectors.VectorUtils;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.Train;

/**
 * This is an improved version of LibLinearModel.
 *
 */
@SuppressWarnings("serial")
public class RawFeaturesLibLinearModel extends BaseLibLinearModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(RawFeaturesLibLinearModel.class);

    // Data we need to save to recreate the model
    private List<String> _uniqueTerms;
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	super.readFields(in);
    	
        _uniqueTerms = readStrings(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	super.write(out);

        writeStrings(out, _uniqueTerms);
    }
    
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((_uniqueTerms == null) ? 0 : _uniqueTerms.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		RawFeaturesLibLinearModel other = (RawFeaturesLibLinearModel) obj;
		if (_uniqueTerms == null) {
			if (other._uniqueTerms != null)
				return false;
		} else if (!_uniqueTerms.equals(other._uniqueTerms))
			return false;
		return true;
	}

	@Override
	public double train(boolean doCrossValidation) {

        _uniqueTerms = buildUniqueTerms(_featuresList);
        List<Vector> vectors = new ArrayList<Vector>(_featuresList.size());
        for (Map<String, Integer> termMap : _featuresList) {
            vectors.add(makeNormalizedVector(termMap));
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

        double result = 1.0;
        if (doCrossValidation) {
            double[] target = new double[problem.l];
            LOGGER.info("Cross validating...");
            Linear.crossValidation(problem, param, DEFAULT_NR_FOLD, target);
            int totalCorrect = 0;
            for (int i = 0; i < problem.l; i++) {
                if (target[i] == problem.y[i]) {
                    ++totalCorrect;
                }
            }
            
            LOGGER.debug(String.format("Correct: %d%n", totalCorrect));
            result = (double)totalCorrect/(double)problem.l;
            LOGGER.debug(String.format("Cross Validation Accuracy = %g%%%n", 100.0 * result));
        }
        
        return result;
    }
    
    protected Vector makeNormalizedVector(Map<String, Integer> termMap) {
    	// We assume that _uniqueTerms has been set up, as a sorted list, so
    	// we can use that to create an appropriate vector.
    	Vector result = VectorUtils.makeVector(_uniqueTerms, termMap);
    	getNormalizer().normalize(result);
    	
		return result;
	}
    
    public void train() {
    	train(_crossValidationRequired);
    }
    
    @Override
    public DocDatum classify(TermsDatum datum) {
        Vector docVector = makeNormalizedVector(datum.getTermMap());
        
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
    
    @Override
	public DocDatum[] classifyNResults(TermsDatum datum, int n) {
        Vector docVector = makeNormalizedVector(datum.getTermMap());
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
    
    @Override
    public String getDetails() {
    	StringBuilder result = new StringBuilder(super.getDetails());
    	double[] weights = _model.getFeatureWeights();

    	for (int i = 0; i < _uniqueTerms.size(); i++) {
    		result.append(String.format("\t%s: %f\n", _uniqueTerms.get(i), weights[i]));
    	}
    	
    	return result.toString();
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
    
    private List<String> buildUniqueTerms(List<Map<String, Integer>> featuresList) {
        Set<String> uniqueTerms = new HashSet<String>();
        for (Map<String, Integer> termMap : featuresList) {
            uniqueTerms.addAll(termMap.keySet());
        }
        List<String> sortedTerms = new ArrayList<String>(uniqueTerms);
        Collections.sort(sortedTerms);
        return sortedTerms;
    }

}
