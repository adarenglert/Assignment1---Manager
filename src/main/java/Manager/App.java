package Manager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class App {
    private class Job{
        private String action;
        private String url;

        public Job(String action, String url) {
            this.action = action;
            this.url = url;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }
    private static final String URLS_PACKAGE_KEY = "urls_package";
    private static final String PATH_TO_JOB_FILE = "pathToJobFile";
    private AmazonSQS sqs;
    private AmazonS3 s3;
    private List<Message> localAppMessages;
    private String queueURL,bucketName;

    public App(String bucketName,String localQ_key) {
        this.bucketName = bucketName;
        this.s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
        this.sqs = AmazonSQSClientBuilder.defaultClient();
        this.queueURL = getLocalAppQueue(localQ_key);
    }

    private String getLocalAppQueue(String loc_man_q) {
        String loc_man_q_url = "";
        S3ObjectInputStream s3is = getS3ObjectInputStream(this.bucketName,loc_man_q);
        loc_man_q_url = buildString(s3is);
        try {
            s3is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return loc_man_q_url;
    }

    private S3ObjectInputStream getS3ObjectInputStream(String b_name,String key_name) {
        try {
        System.out.format("Downloading %s from S3 bucket %s...\n", key_name, this.bucketName);
        S3Object o = this.s3.getObject(b_name, key_name);
        S3ObjectInputStream s3is =  o.getObjectContent();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        return null;
    }

    private String buildString(S3ObjectInputStream s3is) {
        byte[] read_buf = new byte[1024];
        final StringBuilder sb = new StringBuilder();
        try {
            while ((s3is.read(read_buf)) > 0) {
                for (byte b : read_buf) {
                    sb.append(b);
                }
            }
            return sb.toString();
        } catch (IOException e) {
        System.err.println(e.getMessage());
        System.exit(1);
        }
        return null;
    }

    private File buildFile(S3ObjectInputStream s3is,String fileName) throws IOException{
        File fromS3 = new File(fileName);
        FileOutputStream fos = new FileOutputStream(fromS3);
        byte[] read_buf = new byte[1024];
        int read_len = 0;
        while ((read_len = s3is.read(read_buf)) > 0) {
            fos.write(read_buf, 0, read_len);
        }
        fos.close();
        return fromS3;
    }

    private void getMessages(){
        this.localAppMessages = this.sqs.receiveMessage(this.queueURL).getMessages();
        while(this.localAppMessages.size()<1){
            this.localAppMessages = this.sqs.receiveMessage(this.queueURL).getMessages();
        }
    }

    private void deliverJobsToWorkers() {
        for(Message m: this.localAppMessages){
            File f = getFileFromMessage(m);
            parseJobsFromFile(f);
        }
    }

    private void parseJobsFromFile(File f) {
        try {
            Scanner myReader = new Scanner(f);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                System.out.println(data);
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred reading from the file");
            e.printStackTrace();
        }
    }


    private File getFileFromMessage(Message m) {
        String fileName = getFileNameFromMap(m.getAttributes());
        S3ObjectInputStream s3is = getS3ObjectInputStream(this.bucketName,fileName);
        try {
            File f = buildFile(s3is,fileName);
            s3is.close();
            return f;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new File("");
    }

    private String getFileNameFromMap(Map<String, String> attributes) {
        return attributes.get(PATH_TO_JOB_FILE);
    }

    private void printMessages(){
        for(Message m : this.localAppMessages){
            Map<String,String> att = m.getAttributes();
            System.out.println(att.get(URLS_PACKAGE_KEY));
        }
    }

    public static void main( String[] args ) throws IOException {
        String bucket_name = args[0];
        String localQ_key = args[1];
        App manager = new App(bucket_name,localQ_key);
        //Loading an existing document
        System.out.println( "Manager is Running" );
        manager.getMessages();
        manager.deliverJobsToWorkers();
        manager.printMessages();

    }

}

