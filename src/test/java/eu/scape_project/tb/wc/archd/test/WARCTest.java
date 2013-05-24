/*
 *  Copyright 2012 The SCAPE Project Consortium.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package eu.scape_project.tb.wc.archd.test;

import eu.scape_project.tb.wc.archd.hdreader.ArcInputFormat;
import eu.scape_project.tb.wc.archd.hdreader.ArcRecord;
import java.io.*;
import java.net.URL;
import java.util.Date;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author onbram
 */
public class WARCTest extends TestCase {

    private ArcInputFormat myArcF;
    private FileSplit split;
    private TaskAttemptContext tac;

    public WARCTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {

        super.setUp();

        InputStream is = ARCTest.class.getResourceAsStream("WEB-20130522122740493-00000-3735~scape-desktop~8443.warc.gz");
        File file = eu.scape_project.tb.wc.archd.tools.FileUtils.getTmpFile("archd", "warc.gz");
        FileOutputStream fos = new FileOutputStream(file);
        org.apache.commons.io.IOUtils.copy(is, fos);
        fos.flush();
        fos.close();
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        split = new FileSplit(new Path(file.getAbsolutePath()), 0, file.length(), null);

        myArcF = new ArcInputFormat();
        tac = new TaskAttemptContext(conf, new TaskAttemptID());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test of nextKeyValue method, of class ArcRecordReader.
     */
    public void testNextKeyValue() throws Exception {
        RecordReader<Text, ArcRecord> recordReader = myArcF.createRecordReader(split, tac);
        recordReader.initialize(split, tac);
        int start = 1;
        while (recordReader.nextKeyValue()) {
            Text currKey = recordReader.getCurrentKey();
            ArcRecord currValue = recordReader.getCurrentValue();

            String currMIMEType = currValue.getMimeType();
            String currType = currValue.getType();
            String currURL = currValue.getUrl();
            InputStream currStream = currValue.getContents();
            String currContent;
            String myContentString;
            int myContentStringIndex;
            Date currDate = currValue.getDate();
            int currHTTPrc = currValue.getHttpReturnCode();
            int currLength = currValue.getLength();

            System.out.println("KEY " + start + ": " + currKey + " MIME Type: " + currMIMEType + " Type: " + currType + " URL: " + currURL + " Date: " + currDate.toString() + " HTTPrc: " + currHTTPrc + " Length: " + currLength);

            // check example record 1 (first one and the header of the WARC file)
            if (start == 1) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.                
                currContent = content2String(currStream);
                myContentString = "isPartOf: basic";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("ID not equal", "<urn:uuid:18cfb53d-1c89-4cc6-863f-e5535d430c95>", currKey.toString());
                assertEquals("MIME Type not equal", "application/warc-fields", currMIMEType);
                assertEquals("Response type not equal", "warcinfo", currType);
                assertEquals("URL not equal", null, currURL);
                assertTrue("Date not correct", currDate.toString().startsWith("Wed May 22 12:27:40"));
                assertEquals("HTTPrc not equal", -1, currHTTPrc);
                assertEquals("Record length not equal", 374, currLength);
                assertEquals("Content mismatch", 202, myContentStringIndex);
            }
            start++;
        }
    }

    private String content2String(InputStream contents) throws IOException {
        StringWriter myWriter = new StringWriter();
        IOUtils.copy(contents, myWriter, null);
        String out = myWriter.toString();
        return out;
    }
}
