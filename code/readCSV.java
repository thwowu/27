public static void main(String[] args) throws IOException {
    JobConf conf = new JobConf(FundingJob.class);

    conf.setJobName("Funding Data");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setMapperClass(FundingMapper.class);
    conf.setNumReduceTasks(0);

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
}
}


public class FundingMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
private Text market = new Text();
private Text amount = new Text();

public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
    String line = value.toString();
    CSVReader R = new CSVReader(new StringReader(line));

    String[] ParsedLine = R.readNext();
    R.close();

    amount.set(ParsedLine[15]);
    market.set(ParsedLine[3]);

    output.collect(market, amount);



