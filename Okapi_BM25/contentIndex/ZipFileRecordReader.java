 
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
/**
 * Used to iterate through records in a given split.
 * In this particular case, it will be used to read a zip file. As the input will not be split.
 * 
 * @see ZipFileInputFormat
 * @author Jteso
 *
 */
public class ZipFileRecordReader extends RecordReader<Text,BytesWritable>
{
    private FSDataInputStream fsin;
    private ZipInputStream zip;
    private Text currentKey;
    private BytesWritable currentValue;
    private boolean isFinished = false;
 
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = ((FileSplit) inputSplit).getPath();
        FileSystem fs = path.getFileSystem(conf);
        fsin = fs.open(path);
        zip = new ZipInputStream(fsin);
    }
 
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        ZipEntry entry = zip.getNextEntry();
        if ( entry == null )
        {
            isFinished = true;
            return false;
        }
        // Set the key 
        currentKey = new Text( entry.getName() );
        
        // Set the value
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] temp = new byte[8192];
        while ( true )
        {
            int bytesRead = zip.read(temp, 0, 8192);
            if ( bytesRead > 0 )
                bos.write(temp, 0, bytesRead);
            else
                break;
        }
        zip.closeEntry();
        currentValue = new BytesWritable( bos.toByteArray() );
        return true;
    }   
 
    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return isFinished ? 1 : 0;
    }
 
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException
    {
        return currentKey;
    }
 
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException
    {
        return currentValue;
    }
 
    @Override
    public void close() throws IOException
    {
        try { zip.close(); } catch (Exception e) { }
        try { fsin.close(); } catch (Exception e) { }
    }
}