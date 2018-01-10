import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

	public static ArrayList<ArrayList<Double>> in = new ArrayList<ArrayList<Double>>();
	public static ArrayList<ArrayList<Double>> centroids = new ArrayList<ArrayList<Double>>();
	public static ArrayList<ArrayList<Double>> dist = new ArrayList<ArrayList<Double>>();
	public static ArrayList<ArrayList<Double>> newCentroid = new ArrayList<ArrayList<Double>>();
	public static HashMap<Integer, ArrayList<Integer>> gIndices = new HashMap<Integer, ArrayList<Integer>>();
	public static int groundTruth[][] = new int[150][150];
	public static int clusterTruth[][] = new int[150][150];
	public static HashMap<Integer, ArrayList<Integer>> groundTruthHm = new HashMap<Integer, ArrayList<Integer>>();

	/*
	 * Function to calculate Euclidean Distance Between Two Data Points
	 */
	public static double euclidDist(ArrayList<Double> a, ArrayList<Double> b) {
		double sum = 0;

		for (int i = 0; i < a.size(); i++) {
			double diff = a.get(i) - b.get(i);
			double sqr = Math.pow(diff, 2);
			sum = sum + sqr;
		}
		return Math.sqrt(sum);
	}

	/*
	 * Function to Generate initial centroids, which can be explicitly specified
	 * or chosen at random. Run as it is for 3 specific initial centroids(2,4,8)
	 * or uncomment the multi-line comment and comment out the centroids.add
	 * statements to generate random centroids
	 * 
	 */
	public static void centroidGen() {
		/*ArrayList<Integer> randomNum = new ArrayList<Integer>();
		for (int i = 0; i < 3; i++) {
			int rn = new Random().nextInt(in.get(i).size());
			if (!randomNum.contains(rn)) {
				randomNum.add(rn);
			} else {
				i--;
				continue;
			}

		}

		for (int i : randomNum) {
			centroids.add(in.get(i));
		}*/
		centroids.add(in.get(2));
		centroids.add(in.get(4));
		centroids.add(in.get(8));

		distCalculation();

	}

	/*
	 * Function to calculate the Distance Matrix
	 */
	public static void distCalculation() {
		for (int i = 0; i < in.size(); i++) {
			ArrayList<Double> temp = new ArrayList<Double>();
			for (int j = 0; j < centroids.size(); j++) {
				temp.add(euclidDist(in.get(i), centroids.get(j)));

			}
			dist.add(temp);
		}
	}

	/*
	 * Mapper Function takes in Input as <Object, Text> and emits <IntWritable,
	 * Text> the function will find the corresponding centroid (minimum
	 * distance) for every data point and emit it
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String a = value.toString();
			InputStream in = new ByteArrayInputStream(a.getBytes("UTF-8"));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = br.readLine()) != null) {
				String[] str = line.split("\t");
				int id = Integer.parseInt(str[0]);
				ArrayList<Double> temp = dist.get(id - 1);
				int centroidIndex = temp.indexOf(Collections.min(temp));
				Text geneIndex = new Text(String.valueOf(id));
				IntWritable cIndex = new IntWritable(centroidIndex);
				context.write(cIndex, geneIndex);
			}
		}
	}

	/*
	 * Reducer function takes the <IntWrotable, Text> output of mapper, which
	 * corresponds to Cluster id's and their associated data points and
	 * calculates a new centroid
	 * 
	 */
	public static class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private Text result = new Text();

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int numOfAttributes = in.get(0).size();
			ArrayList<Integer> gIndex = new ArrayList<Integer>();
			for (Text val : values) {
				gIndex.add(Integer.parseInt(val.toString()) - 1);
			}
			ArrayList<Double> sumList = new ArrayList<Double>();
			for (int i = 0; i < numOfAttributes; i++) {
				double sum = 0.0;

				for (int val : gIndex) {
					ArrayList<Double> temp = in.get(val);
					sum += temp.get(i);
				}

				sumList.add(sum / gIndex.size());

			}
			gIndices.put(Integer.parseInt(key.toString()), gIndex);
			newCentroid.add(sumList);
			result.set(sumList.toString());
			context.write(key, result);
		}
	}

	/*
	 * This function creates the ground truth matrix for calculation of Jaccard
	 * Coefficient
	 * 
	 */
	public static void createGroundTruth() throws NumberFormatException, IOException {
		BufferedReader br = new BufferedReader(new FileReader("/home/hadoop/Desktop/new_dataset_1.txt"));
		String line;
		// Clustering the gene ids together
		while ((line = br.readLine()) != null) {
			String[] str = line.split("\t");
			if (!groundTruthHm.containsKey(Integer.parseInt(str[1]) - 1)) {
				ArrayList<Integer> a = new ArrayList<Integer>();
				a.add(Integer.parseInt(str[0]) - 1);
				groundTruthHm.put(Integer.parseInt(str[1]) - 1, a);
			} else {
				ArrayList<Integer> a = groundTruthHm.get(Integer.parseInt(str[1]) - 1);
				a.add(Integer.parseInt(str[0]) - 1);
				groundTruthHm.put(Integer.parseInt(str[1]) - 1, a);
			}

		}
		createGroundTruth(groundTruthHm, groundTruth);
		br.close();
	}

	public static void createGroundTruth(HashMap<Integer, ArrayList<Integer>> hm, int[][] truth) {

		// Building the GroundTruth Table
		for (int i : hm.keySet()) {
			for (int j = 0; j < hm.get(i).size(); j++) {
				for (int k = 0; k < hm.get(i).size(); k++) {
					int leftIndx = hm.get(i).get(j);
					int rightIndx = hm.get(i).get(k);
					truth[leftIndx][rightIndx] = 1;
					truth[rightIndx][leftIndx] = 1;
				}
			}

		}

	}

	/*
	 * Function to calculate the vlaue of Jaccard Coefficient
	 */
	public static void calcJaccardCoef(int[][] groundTruth, int[][] clusterTruth) {

		double m11 = 0.0;
		double m10 = 0.0;
		double m01 = 0.0;
		double jaccard;
		for (int i = 0; i < 150; i++) {
			for (int j = 0; j < 150; j++) {
				if ((groundTruth[i][j] == 1) && (clusterTruth[i][j] == 1)) {
					m11++;
				}
				if ((groundTruth[i][j] == 0) && (clusterTruth[i][j] == 1)) {
					m01++;
				}
				if ((groundTruth[i][j] == 1) && (clusterTruth[i][j] == 0)) {
					m10++;
				}
			}
		}
		jaccard = m11 / (m11 + m10 + m01);
		System.out.println("Jaccard:: " + jaccard);
	}

	public static void main(String[] args) throws Exception {

		BufferedReader br = new BufferedReader(new FileReader("/home/hadoop/Desktop/new_dataset_1.txt"));
		String line;
		while ((line = br.readLine()) != null) {
			String[] str = line.split("\t");
			ArrayList<Double> list = new ArrayList<Double>();
			for (int i = 2; i < str.length; i++) {
				list.add(Double.parseDouble(str[i]));
			}
			in.add(list);
		}
		br.close();
		centroidGen();
		int itr = 0;
		createGroundTruth();
		while (true) {
			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			Job job = Job.getInstance(conf, "kmeans");
			System.out.println("Itertion :: " + itr);
			job.setJarByClass(KMeans.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			if (centroids.equals(newCentroid)) {
				createGroundTruth(gIndices, clusterTruth);
				System.out.println("Centroids converged  ");
				for (int a : gIndices.keySet()) {
					System.out.println("key   " + a + "  value " + gIndices.get(a));
				}
				break;
			}
			if (newCentroid.size() != 0) {
				centroids.clear();
				centroids.addAll(newCentroid);
				newCentroid.clear();
				gIndices.clear();
				dist.clear();
				distCalculation();

			}
			if (hdfs.exists(new Path(args[1]))) {
				hdfs.delete(new Path(args[1]), true);
			}

			itr++;
			job.waitForCompletion(true);

		}
		calcJaccardCoef(groundTruth, clusterTruth);
		FileWriter writer = new FileWriter("/home/hadoop/Desktop/new_dataset_1_hadoop.csv");

		for (int i : gIndices.keySet()) {

			ArrayList<Integer> dataP = gIndices.get(i);

			for (int j : dataP) {
				ArrayList<Double> data = in.get(j);
				writer.append(String.valueOf(i));
				writer.append(",");
				for (double d : data) {
					writer.append(String.valueOf(d));
					writer.append(",");
				}
				writer.append("\n");

			}

		}
		writer.close();


	}
}