package Hadoop.Mp4ToMp3;
// Thư viện để xử lý Video (Encode).
import it.sauronsoftware.jave.AudioAttributes;
import it.sauronsoftware.jave.Encoder;
import it.sauronsoftware.jave.EncodingAttributes;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
// Class Mapper kế thừa thừa từ MapReduceBase 
// và thực thi giạo diện Mapper<Object, Text, Text, NullWritable>
public class ConvertMapper extends MapReduceBase 
implements Mapper<Object, Text, Text, NullWritable>  {
	/* Hàm Map ( Với 4 tham số : Object key, Text value,
	 * 	OutputCollector output, Reporter reporter)
	 * Object Key: Key Input,
	 * Text value: Value Input, 
	 * OutputCollector output: Tập các Ouput sau khi hàm map thực hiện xong,
	 * Reporter reporter : Report của Map 
	*/
	public void map(Object Key, Text Value,
			OutputCollector output, Reporter reporter) throws IOException {
		// Key này file chứa tập hợp tên các video.
		// Đọc giá trị trong value ra  biến fileValue.
		String fileValue = Value.toString();
		// Chia chuỗi fileValue thành nhiều Token (Kết quả là 1 mảng chứa các tên file video).
		StringTokenizer listFileName = new StringTokenizer(fileValue);
		// Nếu có phần tử trong mảng listFileName
		while(listFileName.hasMoreTokens()) {	
			// Lấy tên video trong mảng listFileName.
			String fileName=listFileName.nextToken();
			// Khai báo đường dẫn tới nơi lưu file mp4 đầu vào
			String FileIn="/home/hduser/Desktop/input_mp4/"+fileName+".mp4";
			// Khai báo đường dẫn chứa file mp3 sau khi chuyển đổi. 
			String FileOut="/home/hduser/Desktop/output_mp3/"+fileName+".mp3";
			// Lấy file mp4 từ đường dẫn FileIn.
			File SourceFile = new File(FileIn);
			// Tạo file output từ đường dẫn FileOut.
			File DestinationFile = new File(FileOut);
			// Kiểm tra xem file input có tồn tại hay không?
			if(!SourceFile.exists()){
				// In thông báo
				System.out.println("Source file does not exist: " + SourceFile.getName());
				// Thoát chương trình
			    System.exit(0);
			}						
			// Tạo thuộc tính cho Audio
			AudioAttributes AudioConvert = new AudioAttributes();
			// Thiết lập phương thức giải mã: libfaac
			AudioConvert.setCodec("libfaac");	
			// Cấu hình cách thức Encode.
			EncodingAttributes EncodeAttribs = new EncodingAttributes();
			// Thiết lập định dạng file đầu ra sau khi encode.
			EncodeAttribs.setFormat("mp3");
			// Thêm thuộc tính Audio vào phần Encode.
			EncodeAttribs.setAudioAttributes(AudioConvert);
			// Tạo đối tượng EncodeVideo: Chuyển đổi mp4 sang mp3.
			Encoder EncodeVideo = new Encoder();				
			try {				
				// Chuyển file mp4 sang mp3 (Từ File nguồn tới file đích với các thiết lập ở trên) 
				EncodeVideo.encode(SourceFile, DestinationFile, EncodeAttribs);
				// 
				//Output của Mapper sẽ chứa đường dẫn của file mp3.
				output.collect(new Text(FileOut), NullWritable.get());							
			} catch (Exception e) {
				// Bắt lỗi
				e.printStackTrace();
			}
				
		}
	}

}
