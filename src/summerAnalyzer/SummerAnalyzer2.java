package summerAnalyzer;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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

public class SummerAnalyzer2 {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(SummerAnalyzer2.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2014004");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 入出力ファイルを指定
		String inputpath = "out/summer1";
		String outputpath = "out/summer2";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static final int TYPE = 0;
		private static final int COUNT_VACATION = 1;
		private static final int COUNT_NOT_VACATION = 2;

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split("\t");

			// 分類を取得
			String type = csv[TYPE];

			// 売り上げを取得
			String countVacation = format(csv[COUNT_VACATION]);
			String countNotVacation = format(csv[COUNT_NOT_VACATION]);

			//売り上げ0は無視
			if(countVacation.equals("0")&&countNotVacation.equals("0")) return;

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new Text(countVacation+"\t"+countNotVacation), new Text(type));
		}

		private String format(String str){
			int length = str.length();
			StringBuffer output = new StringBuffer();
			for(int i=0;i<10-length;i++) output.append("0");
			return output+str;
		}

	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// 同じ売り上げがあれば合わせる
			StringBuffer output = new StringBuffer();
			for (Text value : values) {
				output.append(","+value);
			}

			// emit
			context.write(new Text(key),new Text(output.toString().substring(1)));
		}
	}

}

