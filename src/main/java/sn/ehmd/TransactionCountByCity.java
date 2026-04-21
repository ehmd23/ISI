package sn.ehmd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Analyse 3 — Nombre de transactions par ville (clé = ville index 2, valeur = 1 par ligne).
 * Le reducer somme les 1 : le total par ville est le nombre de transactions.
 */
public class TransactionCountByCity {

    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text city = new Text();
        private static final LongWritable ONE = new LongWritable(1L);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] fields = line.split("\t");
            if (fields.length >= 3) {
                try {
                    city.set(fields[2].trim());
                    context.write(city, ONE);
                } catch (Exception e) {
                    System.err.println("Erreur parsing : " + line);
                }
            }
        }
    }

    public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable v : values) {
                sum += v.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Nombre de transactions par ville");
        job.setJarByClass(TransactionCountByCity.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
