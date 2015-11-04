package com.scaleunlimited.classify.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;

import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.model.BaseLibLinearModel;
import com.scaleunlimited.classify.vectors.BaseNormalizer;
import com.scaleunlimited.classify.vectors.NullNormalizer;
import com.scaleunlimited.classify.vectors.SetNormalizer;
import com.scaleunlimited.classify.vectors.TfNormalizer;
import com.scaleunlimited.classify.vectors.UnitNormalizer;

public abstract class BaseLibLinearModelTest {

    private static final int NUM_DOCS = 10;
    
    private static final String TRAIN_MAGIC_FEATURES= "Abracadabra\t0.59\tmagic\t0.8\tcaramba\t0.6";
    private static final String TRAIN_MATH_FEATURES= "sum\t0.59\tmultiplication\t0.8\tdivision\t0.6";
    private static final String TRAIN_ELECTROMAGNETIC_FEATURES= "electromagnet\t0.89\tflash\t0.8\tlight\t0.6";
    private static final String TRAIN_MIXED_FEATURES= "Abracadabra\t0.59\tmagic\t0.8\telectromagnet\t0.89";

    private static final String TEST_MAGIC_FEATURES= "magic\t0.8";
    private static final String TEST_MATH_FEATURES= "multiplication\t0.8";

    BaseLibLinearModel _model;

    @Before
    public void setUp() throws Exception {
        _model = getModel();
    }

    abstract protected BaseLibLinearModel getModel();
    
    protected void testModel() throws Exception {
        
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeTermsDatum("magic", TRAIN_MAGIC_FEATURES));
        }
        
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeTermsDatum("math", TRAIN_MATH_FEATURES));
        }
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeTermsDatum("electromagnetic", TRAIN_ELECTROMAGNETIC_FEATURES));
        }
        _model.train();
        
        validateModel(_model);
        
        // Now, to test serialization, save it, read it back in, and verify we get
        // the same results.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        _model.write(out);
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInput in = new DataInputStream(bais);
        BaseLibLinearModel newModel = getModel();
        newModel.readFields(in);
        
        validateModel(newModel);
    }
    
    private void validateModel(BaseLibLinearModel model) {
        TermsDatum magicDoc = makeTermsDatum("magic", TEST_MAGIC_FEATURES);
        TermsDatum mathDoc = makeTermsDatum("math", TEST_MATH_FEATURES);
        TermsDatum emDoc = makeTermsDatum("electromagnetic", TRAIN_ELECTROMAGNETIC_FEATURES);
        TermsDatum mixedDoc = makeTermsDatum("magic", TRAIN_MIXED_FEATURES);

        Assert.assertEquals("Magic doc failed",
                            "magic",
                            model.classify(magicDoc).getLabel());
        Assert.assertEquals("Math doc failed",
                            "math",
                            model.classify(mathDoc).getLabel());
        DocDatum[] nResults = model.classifyNResults(magicDoc, 2);
        Assert.assertEquals(2, nResults.length);
        Assert.assertEquals("Magic doc failed",
                        "magic",
                        nResults[0].getLabel());

        nResults = model.classifyNResults(emDoc, 2);
        Assert.assertEquals(2, nResults.length);
        Assert.assertEquals("Electromagnetic doc failed",
                        "electromagnetic",
                        nResults[0].getLabel());
        
        nResults = model.classifyNResults(mixedDoc, 3);
        Assert.assertEquals(3, nResults.length);
        Assert.assertEquals("Mixed doc failed",
                        "magic",
                        nResults[0].getLabel());
        Assert.assertEquals( "electromagnetic",
                        nResults[1].getLabel());
        
        nResults = model.classifyNResults(mixedDoc, 100);
        Assert.assertEquals(3, nResults.length);
    }
    
    protected void testGettingDetails() throws Exception {
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeTermsDatum("magic", TRAIN_MAGIC_FEATURES));
        }
        
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeTermsDatum("math", TRAIN_MATH_FEATURES));
        }
        for (int i = 0; i < NUM_DOCS; i++) {
            _model.addTrainingTerms(makeTermsDatum("electromagnetic", TRAIN_ELECTROMAGNETIC_FEATURES));
        }
        _model.train();

        System.out.println(_model.getDetails());
    }
    
    protected void testSerializationWithAllNormalizers() throws Exception {
        testSerialization(NullNormalizer.class);
        testSerialization(UnitNormalizer.class);
        testSerialization(SetNormalizer.class);
        testSerialization(TfNormalizer.class);
    }
    
    private void testSerialization(Class<? extends BaseNormalizer> clazz) throws Exception {
        BaseLibLinearModel model1 = getModel();
        model1.setNormalizerClassname(clazz);
        
        model1.addTrainingTerms(makeTermsDatumFromText("good", "This is an example of some good text"));
        model1.addTrainingTerms(makeTermsDatumFromText("bad", "This has lots of bad words in it"));
        
        model1.train();
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        model1.write(out);
        baos.close();
        
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInput in = new DataInputStream(bais);
        BaseLibLinearModel model2 = getModel();
        model2.readFields(in);
        
        Assert.assertEquals(model1, model2);
    }


    private TermsDatum makeTermsDatumFromText(String label, String text) {
    	Map<String, Integer> termsMap = new HashMap<>();
    	for (String term : text.split(" ")) {
    		Integer count = termsMap.get(term);
    		if (count == null) {
    			termsMap.put(term, 1);
    		} else {
    			termsMap.put(term, count + 1);
    		}
    	}
    	
		return new TermsDatum(termsMap, label);
	}

	private TermsDatum makeTermsDatum(String label, String features) {
        Map<String, Integer> termsMap = new HashMap<String, Integer>();
        String[] split = features.split("\t");
        for (int i = 0; i < split.length; i++) {
        	// We get data in the form of <term><tab><term frequency>, so convert that into
        	// term and term count
        	termsMap.put(split[i], (int)Math.round(100.0 * Double.parseDouble(split[i+1])));
            i++;
        }
        
        return new TermsDatum(termsMap, label);
    }
   
    
}
