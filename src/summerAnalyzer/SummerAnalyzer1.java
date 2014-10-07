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

public class SummerAnalyzer1 {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(SummerAnalyzer1.class);       // ★このファイルのメインクラスの名前
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
		String inputpath = "posdata";
		String outputpath = "out/summer1";     // ★MRの出力先
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
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			//明らかなノイズ日は除く
			if(isNoisyDay(csv[PosUtils.MONTH],csv[PosUtils.DATE]))return;

			//若いののみ受付
			if(!isEqual(csv[PosUtils.BUYER_AGE],1,2)) return;

			//夕方のみ受付
			if(!isRange(csv[PosUtils.HOUR],16,20)) return ;

			//夏休み(8月)のみ受付
			if(!isEqual(csv[PosUtils.MONTH],8)) return;

			// valueとなる販売個数を取得
			String count = csv[PosUtils.ITEM_TOTAL_PRICE];

			// keyを取得
			String name = csv[PosUtils.ITEM_CATEGORY_NAME];

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new Text(name), new Text(count));
		}

		private static boolean isEqual(String str,int num){
			return Integer.valueOf(str)==num;
		}
		private static boolean isEqual(String str,int num1,int num2){
			return (Integer.valueOf(str)==num1)||(Integer.valueOf(str)==num2);
		}

		private static boolean isRange(String str,int num1,int num2){
			return (Integer.valueOf(str)>=num1)&&(Integer.valueOf(str)<=num2);
		}

		private static boolean isNoisyDay(String month,String date){
			if((isEqual(month,1)&&isEqual(date,1))||
					(isEqual(month,12)&&isEqual(date,24,25))||
					(isEqual(month,2)&&isEqual(date,14))||
					(isEqual(month,3)&&isEqual(date,14))) return true;
			else return false;
		}

	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// 売り上げを合計
			long count = 0;
			for (Text value : values) {
				count += Long.valueOf(value.toString());
			}

			// emit
			context.write(new Text(key),new Text(String.valueOf(count)));
		}
	}

}

