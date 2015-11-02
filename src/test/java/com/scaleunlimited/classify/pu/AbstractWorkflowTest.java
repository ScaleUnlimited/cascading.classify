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
import java.util.Random;

import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.local.LocalPlatform;
import com.scaleunlimited.classify.datum.TermsDatum;

public abstract class AbstractWorkflowTest {

    private static final long CASCADING_LOCAL_JOB_POLLING_INTERVAL = 100;

    protected static final String WORKING_DIR = "build/pu-test-working/";
    protected static final int NUM_POSITIVE_DATUMS = 100;
    protected static final int NUM_UNLABELED_DATUMS = 1000;
    protected static final String POSITIVE_TERM = "lovely";
    protected static final String DOUBLE_FREQUENCY_TERM = "digusting";
    
    protected LocalPlatform _platform;
    protected Random _random;
    protected BasePath _workingDirPath;
    protected BasePath _positivePath;
    protected BasePath _unlabeledPath;
    protected BasePath _positiveTermsPath;
    protected BasePath _unlabeledTermsPath;
    protected BasePath _spiesPath;
    protected BasePath _spyModelPath;
    protected BasePath _thresholdPath;
    protected BasePath _reliablyNegativeTermsPath;
    protected BasePath _modelPath;

    public AbstractWorkflowTest() {
        super();
    }

    public void setUp() throws Exception {
        _platform = new LocalPlatform(AbstractWorkflowTest.class);
        _platform.setJobPollingInterval(CASCADING_LOCAL_JOB_POLLING_INTERVAL);
        
        _random = new Random(0);
        _workingDirPath = _platform.makePath(WORKING_DIR);
        _workingDirPath.mkdirs();
        _workingDirPath.assertExists("Working directory");
        _positivePath = _platform.makePath(_workingDirPath, ClassifyPUConfig.POSITIVE_SUBDIR_NAME);
        _unlabeledPath = _platform.makePath(_workingDirPath, ClassifyPUConfig.UNLABELED_SUBDIR_NAME);
        _positiveTermsPath = _platform.makePath(_workingDirPath, ClassifyPUConfig.POSITIVE_TERMS_SUBDIR_NAME);
        _unlabeledTermsPath = _platform.makePath(_workingDirPath, ClassifyPUConfig.UNLABELED_TERMS_SUBDIR_NAME);
        _spiesPath = _platform.makePath(_workingDirPath, ClassifyPUConfig.SPIES_SUBDIR_NAME);
        _spyModelPath = _platform.makePath(_workingDirPath, ClassifyPUConfig.SPY_MODEL_SUBDIR_NAME);
        _thresholdPath = _platform.makePath(_workingDirPath, ClassifyPUConfig.RELIABLY_NEGATIVE_THRESHOLD_SUBDIR_NAME);
        _reliablyNegativeTermsPath = _platform.makePath(_workingDirPath, ClassifyPUConfig.RELIABLY_NEGATIVE_TERMS_SUBDIR_NAME);
        _modelPath = _platform.makePath(_workingDirPath, ClassifyPUConfig.MODEL_SUBDIR_NAME);
    }

    protected Iterator<TupleEntry> openTermsSource(BasePath termsPath)
        throws Exception {
        
        Tap termsSource = _platform.makeTap(_platform.makeBinaryScheme(TermsDatum.FIELDS), termsPath);
        return termsSource.openForRead(_platform.makeFlowProcess());
    }

}