/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class WhoAmI extends Configured implements Tool {

  private static final String[] WHOAMI = new String[]{ "whoami" };

  public static class CustomMapper extends Mapper<Text, Text, Text, IntWritable> {

    @Override
    public void map( Text key, Text value, Context context ) throws IOException, InterruptedException {
      Text username = new Text( "user.name=" + System.getProperty( "user.name" ) );
      context.write( username, new IntWritable( 1 ) );

      Shell.ShellCommandExecutor shell = new Shell.ShellCommandExecutor( WHOAMI );
      shell.execute();
      Text whoami = new Text( "whoami=" + shell.getOutput() );
      context.write( whoami, new IntWritable( 1 ) );
    }

  }

  public static class CustomReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce( Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
      int sum = 0;
      for( IntWritable val : values ) {
        sum += val.get();
      }
      result.set( sum );
      context.write( key, result );
    }

  }

  /**
   * Return a single record (filename, "") where the filename is taken from the file split.
   */
  public static class CustomRecordReader extends RecordReader<Text, Text> {

    Path name;
    Text key = null;
    Text value = new Text();

    public CustomRecordReader( Path path ) {
      name = path;
    }

    @Override
    public void initialize( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() {
      if( name != null ) {
        key = new Text( name.getName() );
        name = null;
        return true;
      }
      return false;
    }

    @Override
    public Text getCurrentKey() {
      return key;
    }

    @Override
    public Text getCurrentValue() {
      return value;
    }

    @Override
    public void close() {
    }

    @Override
    public float getProgress() {
      return 0.0f;
    }
  }

  /**
   * A custom input format that creates virtual inputs of a single string for each map.
   */
  public static class CustomInputFormat extends InputFormat<Text, Text> {

    /**
     * Generate the requested number of file splits, with the filename set to the filename of the output file.
     */
    @Override
    public List<InputSplit> getSplits( JobContext job ) throws IOException {
      List<InputSplit> result = new ArrayList<InputSplit>();
      Path outDir = FileOutputFormat.getOutputPath( job );
      int numSplits = job.getConfiguration().getInt( MRJobConfig.NUM_MAPS, 1 );
      for( int i = 0; i < numSplits; ++i ) {
        result.add( new FileSplit( new Path( outDir, "whoami-split-" + i ), 0, 1, null ) );
      }
      return result;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader( InputSplit split, TaskAttemptContext context )
        throws IOException, InterruptedException {
      return new CustomRecordReader( ( (FileSplit)split).getPath() );
    }

  }

  @Override
  public int run( String[] args ) throws Exception {

    if( args.length == 0 ) {
      return printUsage();
    }

    Configuration conf = getConf();
    JobClient client = new JobClient( conf );
    ClusterStatus cluster = client.getClusterStatus();

    int numMaps = cluster.getTaskTrackers();
    if( args.length > 1 ) {
      try {
        numMaps = Integer.parseInt( args[1] );
      } catch( NumberFormatException e ) {
      }
    }
    conf.setInt( MRJobConfig.NUM_MAPS, numMaps );

    Job job = new Job( conf );
    job.setJarByClass( WhoAmI.class );
    job.setJobName( "whoami" );
    job.setInputFormatClass( CustomInputFormat.class );
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( IntWritable.class );
    job.setMapperClass( CustomMapper.class );
    job.setCombinerClass( CustomReducer.class );
    job.setReducerClass( CustomReducer.class );
    FileOutputFormat.setOutputPath( job, new Path( args[ 0 ] ) );

    System.out.println( "System Properties" );
    System.getProperties().list( System.out );
    System.out.println( "user.name=" + System.getProperty( "user.name" ) );

    Date startTime = new Date();
    System.out.println( "Job started: " + startTime );
    int ret = job.waitForCompletion( true ) ? 0 : 1;
    Date endTime = new Date();
    System.out.println( "Job ended: " + endTime );
    System.out.println( "The job took " + ( endTime.getTime() - startTime.getTime() ) / 1000 + " seconds." );

    return ret;
  }

  public static int printUsage() {
    System.out.println( "whoami <output>" );
    ToolRunner.printGenericCommandUsage( System.out );
    return 1;
  }

  public static void main( String[] args ) throws Exception {
    int res = ToolRunner.run( new Configuration(), new WhoAmI(), args );
    System.exit( res );
  }

}
