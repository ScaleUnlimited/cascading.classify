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
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.vectors.BaseNormalizer;
import com.scaleunlimited.classify.vectors.TfNormalizer;
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
public class LibLinearModel extends BaseModel<TermsDatum> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LibLinearModel.class);

    private List<String> _labelNames;
    private List<String> _uniqueTerms;
    private String _modelString;
    private String _normalizerClassname;
    
    private transient BaseNormalizer _normalizer;
    private transient List<String> _labelList;
    private transient List<Map<String, Integer>> _termsList;
    private transient Model _model;
    
    public LibLinearModel() {
        this(TfNormalizer.class);
    }
    
    public LibLinearModel(Class<? extends BaseNormalizer> normalizerClass) {
        super();
        
        _normalizerClassname = normalizerClass.getCanonicalName();
    }

    @Override
    public void reset() {
        _labelList = new ArrayList<String>();
        _termsList = new ArrayList<Map<String, Integer>>();
        _model = null;
    }

    @Override
    public void addTrainingTerms(TermsDatum termsDatum) {
        _model = null;
        _modelString = null;
        _labelList.add(termsDatum.getLabel());
        _termsList.add(termsDatum.getTermMap());
    }

    protected BaseNormalizer getNormalizer() {
        try {
            if (_normalizer == null) {
                Class<? extends BaseNormalizer> normalizerClass = (Class<? extends BaseNormalizer>)Class.forName(_normalizerClassname);
                _normalizer = normalizerClass.newInstance();
            }
            
            return _normalizer;
        } catch (Exception e) {
            throw new RuntimeException("Can't instantiate normalizer: " + _normalizerClassname, e);
        }
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        _normalizerClassname = in.readUTF();
        _labelNames = readStrings(in);
        _uniqueTerms = readStrings(in);
        _modelString = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(_normalizerClassname);
        writeStrings(out, _labelNames);
        writeStrings(out, _uniqueTerms);
        Text.writeString(out, getModelString());
    }

    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_labelNames == null) ? 0 : _labelNames.hashCode());
        result = prime * result + ((_modelString == null) ? 0 : _modelString.hashCode());
        result = prime * result + ((_normalizerClassname == null) ? 0 : _normalizerClassname.hashCode());
        result = prime * result + ((_uniqueTerms == null) ? 0 : _uniqueTerms.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LibLinearModel other = (LibLinearModel) obj;
        if (_labelNames == null) {
            if (other._labelNames != null)
                return false;
        } else if (!_labelNames.equals(other._labelNames))
            return false;
        if (_modelString == null) {
            if (other._modelString != null)
                return false;
        } else if (!_modelString.equals(other._modelString))
            return false;
        if (_normalizerClassname == null) {
            if (other._normalizerClassname != null)
                return false;
        } else if (!_normalizerClassname.equals(other._normalizerClassname))
            return false;
        if (_uniqueTerms == null) {
            if (other._uniqueTerms != null)
                return false;
        } else if (!_uniqueTerms.equals(other._uniqueTerms))
            return false;
        return true;
    }

    public void train() {
        _uniqueTerms = buildUniqueTerms(_termsList);
        List<Vector> vectors = new ArrayList<Vector>(_termsList.size());
        for (Map<String, Integer> termMap : _termsList) {
            vectors.add(VectorUtils.makeVector(_uniqueTerms, termMap));
        }
        getNormalizer().normalize(vectors);
        
        _labelNames = new ArrayList<String>();
        for (String label : _labelList) {
            if (!(_labelNames.contains(label))) {
                _labelNames.add(label);
            }
        }
        Collections.sort(_labelNames);
        List<Double> labelIndexList = new ArrayList<Double>(_labelNames.size());
        for (String label : _labelList) {
            // TODO CSc This could be made more efficient than a linear search
            labelIndexList.add((double)_labelNames.indexOf(label));
        }
        
        Problem problem = Train.constructProblem(   labelIndexList,
                                                    getFeaturesList(vectors),
                                                    vectors.get(0).size(),
                                                    -1.0);
        Parameter param = new Parameter(SolverType.L2R_LR, 10, 0.01);
        _model = Linear.train(problem, param);
        double[] target = new double[_termsList.size()];
        Linear.crossValidation(problem, param, 10, target);
    }
    
    public DocDatum classify(TermsDatum datum) {
        Vector docVector = VectorUtils.makeVector(_uniqueTerms, datum.getTermMap());
        getNormalizer().normalize(docVector);
        FeatureNode[] features = vectorToFeatures(docVector);
        double[] prob_estimates = new double[_labelNames.size()];
        if (_model == null) {
            try {
                _model = modelFromString(_modelString);
            } catch (IOException e) {
                LOGGER.error("Unable to deserialize model from string", e);
            }
        }
        
        int labelIndex = (int)Linear.predictProbability( _model,
                                                    features,
                                                    prob_estimates);
        String labelName = _labelNames.get(labelIndex);
        
        int modelLabelIndexes[] = _model.getLabels();
        float score = 0;
        // TODO CSc This could be made more efficient than a linear search
        for (int i = 0; i < modelLabelIndexes.length; i++) {
            if (modelLabelIndexes[i] == labelIndex) {
                score = (float)(prob_estimates[i]);
            }
        }
        return new DocDatum(labelName, score);
    }
    
    private String getModelString() throws IOException {
        if (_modelString == null) {
            if (_model == null) {
                train();
            }
            StringWriter writer = new StringWriter();
            Linear.saveModel(writer, _model);
            _modelString = writer.toString();
        }
        return _modelString;
    }
    
    private Model modelFromString(String modelString) throws IOException {
        Model result = null;
        if (modelString != null) {
            StringReader reader = new StringReader(modelString);
            LOGGER.debug("Deserializing LibLinear model");
            result = Linear.loadModel(reader);
            LOGGER.debug("LibLinear model ready");
        }
        return result;
    }

    private List<Feature[]> getFeaturesList(List<Vector> vectors) {
        List<Feature[]> result = new ArrayList<Feature[]>();
        for (Vector vector : vectors) {
            Feature[] x = vectorToFeatures(vector);
            result.add(x);
        }
        return result;
    }

    private FeatureNode[] vectorToFeatures(Vector vector) {
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
    
    private List<String> buildUniqueTerms(List<Map<String, Integer>> termsList) {
        Set<String> uniqueTerms = new HashSet<String>();
        for (Map<String, Integer> termMap : termsList) {
            uniqueTerms.addAll(termMap.keySet());
        }
        List<String> sortedTerms = new ArrayList<String>(uniqueTerms);
        Collections.sort(sortedTerms);
        return sortedTerms;
    }

}
