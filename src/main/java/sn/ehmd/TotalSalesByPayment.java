package sn.ehmd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Analyse 1 — CA total par moyen de paiement (champ index 5), montant en index 4.
 * Entrée : même format tabulé que purchases.txt du TP2.
 */
public class TotalSalesByPayment {

    public static class PaymentMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text payment = new Text();
        private final DoubleWritable amount = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] fields = line.split("\t");
            if (fields.length >= 6) {
                try {
                    payment.set(fields[5].trim());
                    amount.set(Double.parseDouble(fields[4].trim()));
                    context.write(payment, amount);
                } catch (Exception e) {
                    System.err.println("Erreur parsing : " + line);
                }
            }
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable total = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable v : values) {
                sum += v.get();
            }
            total.set(sum);
            context.write(key, total);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total CA par moyen de paiement");
        job.setJarByClass(TotalSalesByPayment.class);
        job.setMapperClass(PaymentMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
