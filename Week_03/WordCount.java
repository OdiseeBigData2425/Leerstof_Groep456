
// DIT MOET JE ZELF NIET KUNNEN!!!!!!!!


// dit schrijft onderstaande cell naar een file in de huidige directory
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // De mapper klasse -> Mapper extends 
    // -> de vier klassen tussen <> staan voor (input_key_type, input_value_type, output_key_type, output_value_type)
    // input_value = 1 lijn -> wordt lijn per lijn uitgevoerd
    // output_key -> text namelijk 1 woord
    // output_value -> 1 (maar een speciaal type namelijk IntWritable)
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            // deze functie wordt lijn per lijn opgeroepen
            // Splits op spaties om woord per woord te overlopen
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                // itereer over alle woorden in de lijn
                // stel de waarde van de key in
                word.set(itr.nextToken());
                // emit de key-value paar (word, 1)
                context.write(word, one);
            }

        }
    }

    // Dit is de klasse voor de reducer functionality, opnieuw de types van input en output key-value tussen de <>
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            // Tel alle waarden op in de values array
            int som = 0;
            for(IntWritable val: values){
                som += val.get();
            }
            // zet het resultaat
            result.set(som);
            // emit het key-value paar als resultaat
            context.write(key, result);
        }
    }

    // configure the MapReduce program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count java");
        job.setJarByClass(WordCount.class);
        // configure mapper
        job.setMapperClass(TokenizerMapper.class);
        // configure combiner (soort van reducer die draait op mapping node voor performantie)
        job.setCombinerClass(IntSumReducer.class);
        // configure reducer
        job.setReducerClass(IntSumReducer.class);
        // set output key-value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // set input file (first argument passed to the program)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set output file  (second argument passed to the program)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // In this case, we wait for completion to get the output/logs and not stop the program to early.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
