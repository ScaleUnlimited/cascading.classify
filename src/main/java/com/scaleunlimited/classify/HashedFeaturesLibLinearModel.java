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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.FeaturesDatum;
import com.scaleunlimited.classify.vectors.BaseNormalizer;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;
import de.bwaldvogel.liblinear.Train;

@SuppressWarnings("serial")
public class HashedFeaturesLibLinearModel extends BaseModel<FeaturesDatum> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashedFeaturesLibLinearModel.class);

    private static final SolverType DEFAULT_SOLVER_TYPE = SolverType.L2R_LR;
    private static final double DEFAULT_C = 10;
    private static final double DEFAULT_EPS = 0.01;

    private static final int DEFAULT_NR_FOLD = 5;   // Used when cross validating

    // Just in case we have less than 10x this many unique features, don't constrain
    // it too much
	private static final int MIN_FEATURE_SIZE = 10;
    
    private SolverType _solverType = DEFAULT_SOLVER_TYPE;
    private double _constraintsViolation = DEFAULT_C;
    private double _eps = DEFAULT_EPS;

    private float _percentReduction;
    private List<String> _labelNames;
    private int _modelLabelIndexes[] = null;
    private int _maxFeatureIndex;
    
    private transient List<String> _labelList;
    private transient List<Map<String, Double>> _featuresList;
    private transient Model _model;

    private boolean _crossValidationRequired = true;
    private boolean _quietMode = false;
    private boolean _averageCollisions = true;
    
    public HashedFeaturesLibLinearModel() {
        super();
    }

    public HashedFeaturesLibLinearModel(float percentReduction) {
        super();
        
        _percentReduction = percentReduction;
    }
    
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
        _maxFeatureIndex = in.readInt();
        _model = Linear.loadModel(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        LOGGER.info("Saving model...");
        
        // Make sure we've got a model to write out.
        if (_model == null) {
            train();
        }
        
        writeStrings(out, _labelNames);
        out.writeInt(_maxFeatureIndex);
        Linear.saveModel(out, _model);
        LOGGER.info("Model saved successfully");
    }

    public void train() {
    	// First generate list of unique labels, so we can map a label to an index.
        _labelNames = new ArrayList<String>();
        for (String label : _labelList) {
            if (!(_labelNames.contains(label))) {
                _labelNames.add(label);
            }
        }
        Collections.sort(_labelNames);
        
        // Create list that maps from training data set index to label index
        List<Double> labelIndexList = new ArrayList<Double>(_labelList.size());
        for (String label : _labelList) {
            int index = Collections.binarySearch(_labelNames, label);
            if (index >= 0) {
                labelIndexList.add((double)index);
            } else {
                throw new RuntimeException("Index not found for label :" + label);
            }
        }
        
        // Figure out the max index, by counting # of unique features, and reducing
        // down to some percentage of this count. But we want at least MIN_FEATURE_SIZE, so if
        // we're below that, just set it to the # of features - 1 (so some hashing
        // will occur, for testing).
        Set<String> uniqueFeatures = new HashSet<String>();
        for (Map<String, Double> featuresMap : _featuresList) {
        	uniqueFeatures.addAll(featuresMap.keySet());
        }
        _maxFeatureIndex = Math.round(uniqueFeatures.size() * _percentReduction);
    	LOGGER.info(String.format("Setting max feature index to be %d", _maxFeatureIndex));
        if (_maxFeatureIndex < MIN_FEATURE_SIZE) {
        	_maxFeatureIndex = uniqueFeatures.size() - 1;
        	LOGGER.info(String.format("Resetting max feature index to be %d", _maxFeatureIndex));
        }
        
        List<Feature[]> features = new ArrayList<Feature[]>(_featuresList.size());
        for (Map<String, Double> featuresMap : _featuresList) {
        	features.add(getFeatures(featuresMap));
        }
        
        _featuresList.clear();
        
        if (_quietMode) {
            Linear.disableDebugOutput();
        }
        
        LOGGER.info("Constructing problem for training...");
        Problem problem = Train.constructProblem(   labelIndexList,
        											features,
        											_maxFeatureIndex + 1,
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
        Feature[] features = getFeatures(datum.getFeatureMap());
        double[] probEstimates = new double[_labelNames.size()];
        
        int labelIndex = (int)Linear.predictProbability(_model,
                                                    	features,
                                                    	probEstimates);
        String labelName = _labelNames.get(labelIndex);
        
        if (_modelLabelIndexes == null) {
            _modelLabelIndexes = _model.getLabels();
        }
        
        float score = 0;
        // FUTURE CSc This could be made more efficient than a linear search
        for (int i = 0; i < _modelLabelIndexes.length; i++) {
            if (_modelLabelIndexes[i] == labelIndex) {
                score = (float)(probEstimates[i]);
            }
        }
        return new DocDatum(labelName, score);
    }
    
    public DocDatum[] classifyNResults(FeaturesDatum datum, int n) {
        Feature[] features = getFeatures(datum.getFeatureMap());
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
    	StringBuilder result = new StringBuilder();
    	// TODO output extra info about reduction amount?
    	
    	double[] weights = _model.getFeatureWeights();

    	// TODO this isn't of much use, since all we have are weights for hashed indices.
    	for (int i = 0; i < weights.length; i++) {
    		result.append(String.format("\t%f\n", weights[i]));
    	}
    	
    	return result.toString();
    }
    
    public void setQuietMode(boolean quietMode) {
        _quietMode  = quietMode;
    }

    public void setCrossValidation(boolean required) {
        _crossValidationRequired  = required;
    }
    
    public void setAverageCollisions(boolean averageCollisions) {
    	_averageCollisions = averageCollisions;
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
    
	public static int calcHashBuiltin(String term, int modulo) {
		return (int)((term.hashCode() & 0x07FFFFFFF) % modulo);
	}
    

    private static final long[] lookupTable = createLookupTable();
    private static final long HSTART = 0xBB40E64DA205B064L;
    private static final long HMULT = 7664345821815920749L;
    
	private static final long[] createLookupTable() {
  	  long[] byteTable = new long[256];
  	  long h = 0x544B2FBACAAF1684L;
  	  for (int i = 0; i < 256; i++) {
  	    for (int j = 0; j < 31; j++) {
  	      h = (h >>> 7) ^ h;
  	      h = (h << 11) ^ h;
  	      h = (h >>> 10) ^ h;
  	    }
  	    byteTable[i] = h;
  	  }
  	  return byteTable;
  	}
    
	public static int calcHashLCG(String term, int modulo) {
		byte[] data;
    	try {
    		data = term.getBytes("UTF-8");
    	} catch (UnsupportedEncodingException e) {
    		throw new RuntimeException("Impossible exception", e);
    	}

	    long h = HSTART;
	    final long hmult = HMULT;
	    final long[] ht = lookupTable;
	    for (int len = data.length, i = 0; i < len; i++) {
	      h = (h * hmult) ^ ht[data[i] & 0xff];
	    }
	    
		return (int)((h & 0x07FFFFFFF) % modulo);
	}
    
    public static int calcHashJoaat(String term, int modulo) {
    	byte[] key;
    	
    	try {
    		key = term.getBytes("UTF-8");
    	} catch (UnsupportedEncodingException e) {
    		throw new RuntimeException("Impossible exception", e);
    	}
    	
        int hash = 0;
        
        for (byte b : key) {
            hash += (b & 0xFF);
            hash += (hash << 10);
            hash ^= (hash >> 6);
        }
        
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        
        return Math.abs(hash) % modulo;
    }

    /**
     * Given a map from term to score, generate a feature array using
     * _maxFeatureIndex as the max index, based on the hash of the term.
     * 
     * @param terms
     * @param maxIndex
     * @return
     */
    
    private Feature[] getFeatures(Map<String, Double> terms) {
    	
    	List<FeatureNode> features = new ArrayList<FeatureNode>(terms.size());
    	for (String term: terms.keySet()) {
    		int index = calcHashJoaat(term, _maxFeatureIndex);
    		features.add(new FeatureNode(index + 1, terms.get(term)));
    	}
    	
    	if (features.size() == 0) {
    		return new FeatureNode[0];
    	}
    	
    	// Sort features from low to high by index.
    	Collections.sort(features, new Comparator<FeatureNode>() {

			@Override
			public int compare(FeatureNode o1, FeatureNode o2) {
				return o1.index - o2.index;
			}
		});
    	
    	// If we have any that need to be merged (indexes are the same, due to
    	// hash collision) then we have an extra step here.
    	List<FeatureNode> result = new ArrayList<FeatureNode>(features.size());

    	FeatureNode curFeature = null;
    	int numToCombine = 0;
    	
    	for (FeatureNode feature : features) {
    		if ((curFeature != null) && (feature.index == curFeature.index)) {
    			if (_averageCollisions) {
    				numToCombine += 1;
    				curFeature.value += feature.value;
    			}
    			continue;
    		}
    		
    		if (curFeature != null) {
    			curFeature.value /= numToCombine;
    			result.add(curFeature);
    		}
    		
			curFeature = feature;
			numToCombine = 1;
    	}
    	
		if (curFeature != null) {
			curFeature.value /= numToCombine;
			result.add(curFeature);
		}

		return result.toArray(new FeatureNode[result.size()]);
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
