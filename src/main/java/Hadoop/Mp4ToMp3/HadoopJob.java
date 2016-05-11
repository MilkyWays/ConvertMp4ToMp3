package Hadoop.Mp4ToMp3;
// Thư viện để sử dụng Hadoop MapReduce
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class HadoopJob {

	public static void main(String[] args) {
		
		// Tạo Job.
		JobClient Client = new JobClient();
		// Tạo JobConfig
		// Chỉ định class cho việc chạy Job! 
		JobConf Config = new JobConf(HadoopJob.class);
		// Chỉ định Output Key có kiểu là Text.
		Config.setOutputKeyClass(Text.class);
		// Chỉ định Output Value có kiểu là NullWritable. 
		Config.setOutputValueClass(NullWritable.class);	
		// Thêm đường dẫn Input file vào JobConfig (Nơi chứa File được nhập vào khi Job chạy).
		FileInputFormat.addInputPath(Config, new Path("/home/hduser/Desktop/input/"));
		// Thiết lập đường dẫn Output file cho JobConfig (Nơi chứa file khi Job chạy xong). 
		FileOutputFormat.setOutputPath(Config, new Path("/home/hduser/Desktop/output"));
		// Chỉ định Class chứa Mapper Cho Job. 
		Config.setMapperClass(ConvertMapper.class);
		// Thiết lập số công việc cho Reduce: Ở đây là 0. 
		// Ở Chương trình này chúng ta không cần dùng tới Reduce (Vì ta có Input: 1 key,1 value và Output cũng vậy).
		Config.setNumReduceTasks(0);
		// Thiết lập cấu hình cho Job. Job sẽ nhận các cài đặt từ Config (ở trên). 
		Client.setConf(Config);
		// Kiểm lỗi
		try {
			// Bắt đầu chạy Job.
			JobClient.runJob(Config);
		} catch (Exception e) {
			// Nếu có lỗi thì in tất cả các lỗi có liên quan.
			e.printStackTrace();
		}
	}

	}


