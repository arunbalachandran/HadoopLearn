// Arun Balchandran
// Student ID - 1001402679

package edu.uta.cse6331;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable {
	protected short tag;
	protected int index;
	protected double value;

	// empty contructor (why is this used?)
	Elem() {

	}

	Elem(short tg, int ind, double val) {
		tag = tg;
		index = ind;
		value = val;
	}

	public void write(DataOutput out) throws IOException {
		out.writeShort(tag);
		out.writeInt(index);
		out.writeDouble(value);
	}

	public void readFields(DataInput in) throws IOException {
		tag = in.readShort();
		index = in.readInt();
		value = in.readDouble();
	}
}

class Pair implements WritableComparable<Pair> {
	protected int i;
	protected int j;

	// empty contructor (why is this used?)
	Pair() {

	}

	Pair(int index1, int index2) {
		i = index1;
		j = index2;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(i);
		out.writeInt(j);
	}

	public void readFields(DataInput in) throws IOException {
		i = in.readInt();
		j = in.readInt();
	}

	public int compareTo(Pair p) {
        if ((this.i == p.i) && (this.j < p.j)) {
            return -1;
        }
        else if ((this.i == p.i) && (this.j == p.j)){
            return 0;
        }
        else if ((this.i == p.i) && (this.j > p.j)) {
            return 1;
        }
        else if ((this.i < p.i) && (this.j < p.j)) {
            return -1;
        }
        else if ((this.i < p.i) && (this.j == p.j)) {
            return -1;
        }
        else if ((this.i < p.i) && (this.j > p.j)) {
            return -1;
        }
        else if ((this.i > p.i) && (this.j < p.j)) {
            return 1;
        }
        else if ((this.i > p.i) && (this.j == p.j)){
            return 1;
        }
        // else if ((this.i > p.i) && (this.j > p.j)) {
        else {
            return 1;
        }
    }
}

public class Multiply {
	// mappers and reducer for multiplicationJob
	public static class FirstMatrixMapper extends Mapper<Object, Text, IntWritable, Elem> {
		@Override
		public void map(Object key, Text value, Context context)
						throws IOException, InterruptedException {
			// read the contents of the first matrix
			Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
			// indices of the first matrix
			int i = scanner.nextInt();
			int j = scanner.nextInt();
			// value at the indices
			double v = scanner.nextDouble();
			// the tag '0' indicates that the value is being emitted from Matrix 1
			context.write(new IntWritable(j), new Elem((short) 0, i, v));
			scanner.close();
		}
	}

	public static class SecondMatrixMapper extends Mapper<Object, Text, IntWritable, Elem> {
		@Override
		public void map(Object key, Text value, Context context)
						throws IOException, InterruptedException {
			// read the contents of the second matrix
			Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
			// indices of the second matrix
			int i = scanner.nextInt();
			int j = scanner.nextInt();
			// value at the indices
			double v = scanner.nextDouble();
			// the tag '1' indicates that the value is being emitted from Matrix 2
			context.write(new IntWritable(i), new Elem((short) 1, j, v));
			scanner.close();
		}
	}

	// instead of directly passing data, write it to a text file
	public static class MultiplicationReducer extends Reducer<IntWritable, Elem, String, DoubleWritable> {
		@Override
		public void reduce(IntWritable key, Iterable<Elem> values, Context context )
						   throws IOException, InterruptedException {
			// this is a global variable for all reducers
			Vector<Elem> elementsA = new Vector<Elem>();
			Vector<Elem> elementsB = new Vector<Elem>();
			for (Elem v: values) {
				if (v.tag == 0) {
                    Elem temp = new Elem();
                    temp.tag = v.tag;
                    temp.index = v.index;
                    temp.value = v.value;
        			// System.out.println("current elem is " + v.tag + "\t" + v.index + " " + key.get() + " " + v.value);
					elementsA.add(temp);
				}
				else if (v.tag == 1) {
                    Elem temp = new Elem();
                    temp.tag = v.tag;
                    temp.index = v.index;
                    temp.value = v.value;
                    // System.out.println("current elem is " + v.tag + "\t" + key.get() + " " + v.index + " " + v.value);
					elementsB.add(temp);
				}
			}

            // System.out.println("-----------------------------");
            // for (Elem a: elementsA)
            //     System.out.println(a.tag + " " + a.index + " " + a.value);
            // System.out.println("=============================");
            // for (Elem b: elementsB)
            //     System.out.println(b.tag + " " + b.index + " " + b.value);
            // System.out.println("-----------------------------");

			for (Elem b: elementsB) {
				for (Elem a: elementsA) {
                    // System.out.println("Current index is " + a.index + " " + b.index + " for values " + a.value + " * " + b.value);
                    // to print vector
					context.write(a.index + "," + b.index,
								  new DoubleWritable(a.value * b.value));
				}
			}
		}
	}

	// mapper and reducer for addJob
	// this mapper just returns the data it reads without fuss
	public static class AddMapper extends Mapper<Object, Text, Pair, DoubleWritable> {
		@Override
		public void map(Object key, Text value, Context context)
						throws IOException, InterruptedException {
			// read the contents of the first matrix
			Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
			// indices of the result matrix
			int i = scanner.nextInt();
			String string1 = scanner.next();
			String[] arr = string1.split("\\t");
			int j = Integer.parseInt(arr[0]);
			double v = Double.parseDouble(arr[1]);
            // System.out.println("Currently read " + i + " " + j + " " + v);
			context.write(new Pair(i,j), new DoubleWritable(v));
			scanner.close();
		}
	}

	public static class AddReducer extends Reducer<Pair, DoubleWritable, String, DoubleWritable> {
		@Override
		public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
						   throws IOException, InterruptedException {
			double elementSum = 0.0;
			for (DoubleWritable v: values) {
                // System.out.println("The current value is " + key.i + " " + key.j + " " + v);
				elementSum = elementSum + v.get(); // the java equivalent does not do an automatic type
								   // cast when both the elements on the left and right
								   // have differing types
                // System.out.println("The sum is " + elementSum);
			}
            // System.out.println("*_*_*_*_*_*+*+*+*_*_*_*_*_*_*_*_+**+*++_*+*+*_*_");
			context.write(key.i + "," + key.j, new DoubleWritable(elementSum));
		}
	}

	public static void main(String[] args) throws Exception {
		Job multiplicationJob = Job.getInstance();
		multiplicationJob.setJobName("Multiplication Job");
		multiplicationJob.setJarByClass(Multiply.class);  // the name of this class
		// output format for the reducer of the multiplicationJob
		multiplicationJob.setOutputKeyClass(Pair.class);
		multiplicationJob.setOutputValueClass(DoubleWritable.class);
		// output format common to both multiplication mappers
		// this assumes that both input files could be of different formats but are mapped
		// to the same type of mapper output
		multiplicationJob.setMapOutputKeyClass(IntWritable.class);
		multiplicationJob.setMapOutputValueClass(Elem.class);
		multiplicationJob.setReducerClass(MultiplicationReducer.class);
		// format of the 'intermediate' output text file
		multiplicationJob.setOutputFormatClass(TextOutputFormat.class);
		// apparently if you have multiple input files you do not have to setInputFormatClass
		// because it is the first 3rd parameter passed to MultipleInputs.addInputPath
		// so directly setting the paths to the files ... (input and output)
		MultipleInputs.addInputPath(multiplicationJob,
									new Path(args[0]),
									TextInputFormat.class,
									FirstMatrixMapper.class);
		MultipleInputs.addInputPath(multiplicationJob,
									new Path(args[1]),
									TextInputFormat.class,
									SecondMatrixMapper.class);
		// setting the output as the 'intermediate' output
		FileOutputFormat.setOutputPath(multiplicationJob, new Path(args[2]));
		multiplicationJob.waitForCompletion(true);

		// now start the second job
		Job addJob = Job.getInstance();
		addJob.setJobName("Addition Job");
		addJob.setJarByClass(Multiply.class);  // the name of this class
		// output format for the reducer of the addJob
		addJob.setOutputKeyClass(Pair.class);
		addJob.setOutputValueClass(DoubleWritable.class);
		// output format for the addJob mapper (same as mulitiplicationJob reducer)
		addJob.setMapOutputKeyClass(Pair.class);
		addJob.setMapOutputValueClass(DoubleWritable.class);
		addJob.setMapperClass(AddMapper.class);
		addJob.setReducerClass(AddReducer.class);
		// format of the 'intermediate' output text file
		addJob.setInputFormatClass(TextInputFormat.class);
		addJob.setOutputFormatClass(TextOutputFormat.class);
		// the input file path should be changed to the part-* file that is output by the
		// multiplication class
		FileInputFormat.setInputPaths(addJob, new Path(args[2] + "/part-r-00000"));
		// setting the final output path
		FileOutputFormat.setOutputPath(addJob, new Path(args[3]));
		addJob.waitForCompletion(true);
	}
}
