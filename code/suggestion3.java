public class MultiColSumDemo extends Configured implements Tool {

public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MultiColSumDemo(), args);
}

    
    
    
@Override
public int run(String[] arg0) throws Exception {

    getConf().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "  ");

    Job job = Job.getInstance(getConf());

    job.setJobName("MultiColSumDemo");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MultiColMapper.class);

    job.setReducerClass(MultiColReduce.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    FileInputFormat.setInputPaths(job, new Path("input/sum_multi_col"));
    FileOutputFormat.setOutputPath(job, new Path("sum_multi_col_otput" + System.currentTimeMillis()));

    job.setJarByClass(MultiColSumDemo.class);
    job.submit();

    return 1;
}

    
    
    
class MultiColMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(key + " " + value);
        context.write(key, value);
    }

}

    
    
    
class MultiColReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Iterator<Text> it = values.iterator();
        StringTokenizer st = null;
        HashMap<String, Integer> sumCol = new HashMap<>();
        String[] name = null;

        System.out.println(key + " " + values);

        while (it.hasNext()) {
            st = new StringTokenizer(it.next().toString(), "  ");
            st.nextToken();
            while (st.hasMoreTokens()) {
                name = st.nextToken().split("#");
                if (sumCol.get(name[0]) == null)
                    sumCol.put(name[0], Integer.parseInt(name[1]));
                else
                    sumCol.put(name[0], sumCol.get(name[0]) + Integer.parseInt(name[1]));
            }
        }

        StringBuilder sb = new StringBuilder();

        for (Entry<String, Integer> val : sumCol.entrySet())
            sb.append(val.getKey() + "#" + val.getValue());
        System.out.println(key + " " + sb);
        context.write(key, new Text(sb.toString()));

    }
}
}
