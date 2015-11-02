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

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import cascading.flow.Flow;

import com.scaleunlimited.cascading.BaseTool;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;

public class ClassifyDocsTool extends BaseTool {

//    private static final long LOCAL_HADOOP_JOB_POLLING_INTERVAL = 100;

    @SuppressWarnings("rawtypes")
	public static void main(String[] args) {
        ClassifyOptions options = new ClassifyOptions();
        CmdLineParser parser = parse(args, options);
        try {
            parser.parseArgument(args);

            HadoopPlatform platform = new HadoopPlatform(ClassifyDocsTool.class);
            // TODO VMa - figure out polling interval
//            platform.setJobPollingInterval(LOCAL_HADOOP_JOB_POLLING_INTERVAL)
            Flow flow = ClassifyDocsPipe.createTextFlow(platform, options);
            if (options.getDOTFile() != null) {
                flow.writeDOT(getDotFileName(options, "make-clusters"));
                flow.writeStepsDOT(getStepDotFileName(options, "make-clusters"));
            }
            
            flow.complete();
            
        } catch (CmdLineException e)  {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        } catch (Exception e) {
            System.err.println("Exception running tool: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
