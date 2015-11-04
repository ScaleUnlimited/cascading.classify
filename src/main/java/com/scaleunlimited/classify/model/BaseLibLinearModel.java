package com.scaleunlimited.classify.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.vectors.BaseNormalizer;
import com.scaleunlimited.classify.vectors.TfNormalizer;

import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.SolverType;

@SuppressWarnings("serial")
public abstract class BaseLibLinearModel extends BaseModel<TermsDatum> {

    protected static final SolverType DEFAULT_SOLVER_TYPE = SolverType.L2R_LR;
    protected static final double DEFAULT_C = 10;
    protected static final double DEFAULT_EPS = 0.01;

    protected static final int DEFAULT_NR_FOLD = 5;   // Used when cross validating
    
	protected static final String DEFAULT_NORMALIZER_CLASSNAME = TfNormalizer.class.getCanonicalName();
    
    // Data we need to save to recreate the model
    protected List<String> _labelNames;
    protected Model _model;
    private String _normalizerClassname = DEFAULT_NORMALIZER_CLASSNAME;
    
    // We get this from the model
    protected transient int _modelLabelIndexes[] = null;

    // Data used during training
    protected transient SolverType _solverType = DEFAULT_SOLVER_TYPE;
    protected transient double _constraintsViolation = DEFAULT_C;
    protected transient double _eps = DEFAULT_EPS;
    protected transient boolean _crossValidationRequired = true;
    protected transient boolean _quietMode = false;
    protected transient List<String> _labelList;
    protected transient List<Map<String, Integer>> _featuresList;
    
    private transient BaseNormalizer _normalizer;
    
	public BaseLibLinearModel() {
        _labelList = new ArrayList<String>();
        _featuresList = new ArrayList<>();
	}

	// Method specific to LibLinear models (so not in BaseModel)
	public abstract double train(boolean doCrossValidation);

	@Override
	public void reset() {
		super.reset();
		
		_labelList.clear();
		_featuresList.clear();
	}
	
	public BaseNormalizer getNormalizer() {
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
	public void addTrainingTerms(TermsDatum datum) {
        _labelList.add(datum.getLabel());
        _featuresList.add(datum.getTermMap());
    }

	@Override
	public String getDetails() {
		// TODO output info about label names and model
		return "";
	}

    public BaseLibLinearModel setNormalizerClassname(Class<? extends BaseNormalizer> normalizerClass) {
        _normalizerClassname  = normalizerClass.getCanonicalName();
        return this;
    }


    public BaseLibLinearModel setQuietMode(boolean quietMode) {
        _quietMode  = quietMode;
        return this;
    }

    public BaseLibLinearModel setCrossValidation(boolean required) {
        _crossValidationRequired  = required;
        return this;
    }
    
    public BaseLibLinearModel setMultiClassSolverType(boolean multiClassSolverType) {
        if (multiClassSolverType) {
            _solverType = SolverType.MCSVM_CS;
            _eps = 0.1;
        }
        
        return this;
    }
    
    public BaseLibLinearModel setC(double c) {
        _constraintsViolation = c;
        return this;
    }

    public BaseLibLinearModel setEPS(double eps) {
        _eps = eps;
        return this;
    }

    protected Parameter createParameter() {
        return new Parameter(_solverType, _constraintsViolation, _eps);
    }

	@Override
	public void readFields(DataInput in) throws IOException {
        _normalizerClassname = in.readUTF();
        _labelNames = readStrings(in);
        _model = Linear.loadModel(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(_normalizerClassname);
        writeStrings(out, _labelNames);
        Linear.saveModel(out, _model);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((_labelNames == null) ? 0 : _labelNames.hashCode());
		result = prime * result + ((_model == null) ? 0 : _model.hashCode());
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
		BaseLibLinearModel other = (BaseLibLinearModel) obj;
		if (_labelNames == null) {
			if (other._labelNames != null)
				return false;
		} else if (!_labelNames.equals(other._labelNames))
			return false;
		if (_model == null) {
			if (other._model != null)
				return false;
		} else if (!_model.equals(other._model))
			return false;
		return true;
	}

    protected static class LabelIndexScore implements Comparable<LabelIndexScore> {
        
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
