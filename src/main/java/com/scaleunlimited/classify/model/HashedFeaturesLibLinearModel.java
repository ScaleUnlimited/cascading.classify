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
package com.scaleunlimited.classify.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.TermsDatum;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.Train;

@SuppressWarnings("serial")
public class HashedFeaturesLibLinearModel extends BaseLibLinearModel {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashedFeaturesLibLinearModel.class);

    // if num features * percent reduction is less than this, keep all of the features
    // (no reduction)
	private static final int MIN_FEATURE_SIZE = 10;
    
	// We generate this during training
    private int _maxFeatureIndex;
    
    // Values we need during training only, thus not saved
    private transient float _percentReduction = 0.10f;
    private transient boolean _averageCollisions = true;
    
    public HashedFeaturesLibLinearModel() {
        super();
    }

    public HashedFeaturesLibLinearModel setPercentReduction(float percentReduction) {
    	_percentReduction = percentReduction;
    	return this;
    }
    
    public HashedFeaturesLibLinearModel setAverageCollisions(boolean averageCollisions) {
    	_averageCollisions = averageCollisions;
    	return this;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	super.readFields(in);
        _maxFeatureIndex = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	super.write(out);
        out.writeInt(_maxFeatureIndex);
    }

    @Override
    public void train() {
    	train(_crossValidationRequired);
    }
    
    @Override
    public double train(boolean doCrossValidation) {
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
        for (Map<String, Integer> termsMap : _featuresList) {
        	uniqueFeatures.addAll(termsMap.keySet());
        }
        
        _maxFeatureIndex = Math.round(uniqueFeatures.size() * _percentReduction);
    	LOGGER.debug(String.format("Setting max feature index to be %d", _maxFeatureIndex));
        if (_maxFeatureIndex < MIN_FEATURE_SIZE) {
        	_maxFeatureIndex = uniqueFeatures.size() - 1;
        	LOGGER.debug(String.format("Resetting max feature index to be %d", _maxFeatureIndex));
        }
        
        List<Feature[]> features = new ArrayList<Feature[]>(_featuresList.size());
        for (Map<String, Integer> termsMap : _featuresList) {
        	features.add(getFeatures(termsMap));
        }
        
        _featuresList.clear();
        
        if (_quietMode) {
            Linear.disableDebugOutput();
        }
        
        LOGGER.debug("Constructing problem for training...");
        Problem problem = Train.constructProblem(   labelIndexList,
        											features,
        											_maxFeatureIndex + 1,
                                                    -1.0);
        Parameter param = createParameter();
        
        LOGGER.debug("Starting training...");
        _model = Linear.train(problem, param);
        LOGGER.debug(String.format("Trained model with %d classes and %d features", _model.getNrClass(), _model.getNrFeature()));

        double crossValidationAccuracy = 0.0;
        if (doCrossValidation) {
            double[] target = new double[problem.l];
            LOGGER.debug("Cross validating...");
            Linear.crossValidation(problem, param, DEFAULT_NR_FOLD, target);
            int totalCorrect = 0;
            for (int i = 0; i < problem.l; i++) {
                if (target[i] == problem.y[i]) {
                    ++totalCorrect;
                }
            }
            
            crossValidationAccuracy = (double)totalCorrect / (double)problem.l;
            LOGGER.debug(String.format("Correct: %d%n", totalCorrect));
            LOGGER.debug(String.format("Cross Validation Accuracy = %g%%%n", 100.0 * crossValidationAccuracy));
        }
        
        return crossValidationAccuracy;
    }
    
    @Override
    public DocDatum classify(TermsDatum datum) {
        Feature[] features = getFeatures(datum.getTermMap());
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
    
    public DocDatum[] classifyNResults(TermsDatum datum, int n) {
        Feature[] features = getFeatures(datum.getTermMap());
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
    	// TODO output extra info about reduction amount?
    	
    	return result.toString();
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
     * Given a map from term to count, generate a feature array using
     * _maxFeatureIndex as the max index, based on the hash of the term.
     * 
     * @param terms
     * @return array of LibLinear features
     */
    
    private Feature[] getFeatures(Map<String, Integer> terms) {

    	// First create the vector, where each term's index is the hash
    	// of the term, and the value is the term count.
    	Map<Integer, Integer> collisionCount = new HashMap<>();
    	Vector v = new RandomAccessSparseVector(_maxFeatureIndex);
    	for (String term: terms.keySet()) {
    		int index = calcHashJoaat(term, _maxFeatureIndex);
    		double curValue = v.getQuick(index);
    		if (_averageCollisions && (curValue != 0.0)) {
    			Integer curCollisionCount = collisionCount.get(index);
    			if (curCollisionCount == null) {
    				// Number of values we'll need to divide by
    				collisionCount.put(index, 2);
    			} else {
    				collisionCount.put(index, curCollisionCount + 1);
    			}

    			v.setQuick(index, curValue + terms.get(term));
    		} else {
    			v.setQuick(index, terms.get(term));
    		}
    	}

    	// Now adjust the vector for collisions, if needed.
    	if (_averageCollisions && !collisionCount.isEmpty()) {
    		for (Integer index : collisionCount.keySet()) {
    			double curValue = v.getQuick(index);
    			v.setQuick(index, curValue / collisionCount.get(index));
    		}
    	}

    	// Apply the term vector normalizer.
    	getNormalizer().normalize(v);

    	List<FeatureNode> features = new ArrayList<FeatureNode>(terms.size());
    	for (Element e : v.nonZeroes()) {
    		features.add(new FeatureNode(e.index() + 1, e.get()));
    	}

    	// We need to sort by increasing index.
    	Collections.sort(features, new Comparator<FeatureNode>() {

    		@Override
    		public int compare(FeatureNode o1, FeatureNode o2) {
    			return o1.index - o2.index;
    		}
    	});

    	return features.toArray(new FeatureNode[features.size()]);
    }

}
