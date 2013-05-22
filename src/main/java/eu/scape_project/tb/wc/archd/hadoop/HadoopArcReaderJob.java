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
package eu.scape_project.tb.wc.archd.hadoop;

import eu.scape_project.tb.wc.archd.hdreader.ArcInputFormat;
import eu.scape_project.tb.wc.archd.hdreader.ArcRecord;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.Vector;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author onbram
 */
public class HadoopArcReaderJob extends Configured implements Tool {

    // Logger instance
    private static Logger logger = LoggerFactory.getLogger(HadoopArcReaderJob.class.getName());

    public static void main(String[] args) throws Exception {

        logger.info("HADOOP ARC reader test hadoop job.");
        long startTime = System.currentTimeMillis();

        int res = ToolRunner.run(new Configuration(), new HadoopArcReaderJob(), args);

        long elapsedTime = System.currentTimeMillis() - startTime;
        logger.info("Processing time (sec): " + elapsedTime / 1000F);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        HadoopJobCliConfig pc = new HadoopJobCliConfig();
        CommandLineParser cmdParser = new PosixParser();
        CommandLine cmd = cmdParser.parse(HadoopJobOptions.OPTIONS, gop.getRemainingArgs());
        if ((args.length == 0) || (cmd.hasOption(HadoopJobOptions.HELP_OPT))) {
            HadoopJobOptions.exit("Usage", 0);
        } else {
            HadoopJobOptions.initOptions(cmd, pc);
        }
        String dir = pc.getDirStr();

        String name = pc.getHadoopJobName();
        if (name == null || name.equals("")) {
            name = "webarc_reader"; // default job name
        }

        Job job = new Job(conf);

        //**********************************************************
        // for debugging in local mode
        // comment out the 2 lines below befor switching to pseudo-distributed or fully-distributed mode
        // job.getConfiguration().set("mapred.job.tracker", "local");
        // job.getConfiguration().set("fs.default.name", "local");
        //**********************************************************

        FileInputFormat.setInputPaths(job, new Path(dir));
        String outpath = "output/" + System.currentTimeMillis() + "wcr";
        logger.info("Output directory: " + outpath);
        FileOutputFormat.setOutputPath(job, new Path(outpath));

        job.setJarByClass(HadoopArcReaderJob.class);

        job.setJobName(name);

        //*** Set interface data types
        // We are using LONG because this value can become very large on huge archives.
        // In order to use the combiner function, also the map output needs to be a LONG.
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        //*** Set up the mapper, combiner and reducer
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        //*** Set the MAP output compression
        //job.getConfiguration().set("mapred.compress.map.output", "true");

        //*** Set input / output format
        job.setInputFormatClass(ArcInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //*** Start the job and wait for it
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /**
     * The map class of WordCount.
     */
    public static class Map
            extends Mapper<Text, ArcRecord, Text, LongWritable> {

        @Override
        public void map(Text key, ArcRecord value, Context context) throws IOException, InterruptedException {
            String recMimeType = value.getMimeType();
            context.write(new Text(recMimeType), new LongWritable(1));

        }
    }

    /**
     * The reducer class of WordCount
     */
    public static class Reduce
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        long sum;

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
