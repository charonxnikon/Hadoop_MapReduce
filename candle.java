import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.math.BigDecimal;

public class candle {
    public static class CandleMapper
            extends Mapper<Object, Text, Text, Text> {

        private long time2ms(long time){
            long h = time / 10000000;
            long minms = time % 10000000;
            long m = minms / 100000;
            long ms = minms % 100000;
            return ((h * 60 + m) * 60) * 1000 + ms;
        }

        private  long ms2time(long time) {
            long ms = time % 1000;
            long secs = time / 1000;
            long s = secs % 60;
            long mins = secs / 60;
            long m = mins % 60;
            long h = mins / 60;
            return ((h * 100 + m) * 100 + s) * 1000 + ms;
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String regex = conf.get("candle.securities", ".*");
            long width = conf.getLong("candle.width", 300000);
            long date_from = conf.getLong("candle.date.from", 19000101);
            long date_to = conf.getLong("candle.date.to", 20200101);
            long time_from = conf.getLong("candle.time.from", 1000);
            long time_to = conf.getLong("candle.time.to", 1800);

            String[] features = value.toString().split(",");
            String instr = features[0];
            if (instr.equals("#SYMBOL")) {
                return;
            }
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(instr);
            if (matcher.matches()) {
                long moment = Long.parseLong(features[2]);
                long date = moment / 1000000000;
                long time_ms = time2ms(moment % 1000000000);
                long time_from_ms = time2ms(time_from * 100000);
                long time_to_ms = time2ms(time_to * 100000);
                long candle_numbers = (time_to_ms - time_from_ms) / width;
                long last_time_norm = candle_numbers * width;
                long time_ms_norm = time_ms - time_from_ms;

                if ((date_from <= date) && (date < date_to)
                        && (time_ms_norm >= 0) && (time_ms_norm < last_time_norm)) {
                    String deal = features[3];
                    long candle_id = time_ms_norm / width;
                    long candle_moment = ms2time(time_from_ms + candle_id * width);
                    candle_moment = date * 1000000000 + candle_moment;
                    String k = instr + "," + Long.toString(candle_moment);
                    String v = deal + "," + features[4] + "," + Long.toString(ms2time(time_ms)) +
                            "," + Long.toString(candle_id);
                    context.write(new Text(k), new Text(v));
                }
            }
        }
    }

    public static class CandlePartitioner extends
            Partitioner <Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String[] v = value.toString().split(",");
            long candle_id = Long.parseLong(v[3]);
            return (int)(candle_id % numReduceTasks);
        }
    }

    public static class CandleReducer
            extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private MultipleOutputs mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            String symbol = key.toString().split(",")[0];
            long moment_first = 235959999;
            long deal_id_first = Long.MAX_VALUE;
            long moment_last = 0;
            long deal_id_last = 0;
            double high = 0.0;
            double low = Double.MAX_VALUE;
            double open = 0.0;
            double close = 0.0;
            for (Text val : values) {
                String[] obj = val.toString().split(",");
                long deal_id = Long.parseLong(obj[0]);
                double price = Double.parseDouble(obj[1]);
                price = BigDecimal.valueOf(price).setScale(1, BigDecimal.ROUND_HALF_UP).doubleValue();
                long moment = Long.parseLong(obj[2]);
                if (price > high) {
                    high = price;
                }
                if (price < low) {
                    low = price;
                }
                if ((moment < moment_first) || ((moment == moment_first) && (deal_id < deal_id_first))) {
                        moment_first = moment;
                        deal_id_first = deal_id;
                        open = price;
                }
                if ((moment > moment_last) || ((moment == moment_last) && (deal_id > deal_id_last))) {
                        moment_last = moment;
                        deal_id_last = deal_id;
                        close = price;
                }
            }
            String answer = Double.toString(open) + "," + Double.toString(high) + "," +
                    Double.toString(low) + "," + Double.toString(close);
            result.set(answer);
            mos.write(key, result, symbol);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser ops = new GenericOptionsParser(conf, args);
        conf = ops.getConfiguration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        int n = args.length;
        if (n < 2) {
            System.err.println("input/output directory does not found");
            System.exit(1);
        }
        int reducer_numbers = conf.getInt("candle.num.reducers", 1);
        Job job = Job.getInstance(conf);
        job.setJobName("candle");
        job.setJarByClass(candle.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[n - 2]));
        FileOutputFormat.setOutputPath(job, new Path(args[n - 1]));
        job.setMapperClass(CandleMapper.class);
        job.setPartitionerClass(CandlePartitioner.class);
        job.setReducerClass(CandleReducer.class);
        job.setNumReduceTasks(reducer_numbers);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}