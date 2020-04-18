package Manager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


public class App {
    private static final int DEFAULT_WORKERS_RATIO = 100;

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
    private static final String PATH_TO_JOB_FILE = "keyToJobFile";
    private int workersRatio;
    final private AmazonSQS sqs;
    private AmazonS3 s3;
    private List<Message> localAppMessages;
    private List<String> workers;
    private String queueURL,bucketName;

    public App(String bucketName) {
        this.bucketName = bucketName;
        this.s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        this.sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        this.queueURL = getLocalAppQueue(URLS_PACKAGE_KEY);
        this.workersRatio = DEFAULT_WORKERS_RATIO;
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
        S3ObjectInputStream s3is =  o.getObjectContent() ;
        return s3is;
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
                    if(b==0)
                        break;
                    sb.append((char) b);
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
        ReceiveMessageResult messageResult = this.sqs.receiveMessage(this.queueURL);
        this.localAppMessages = messageResult.getMessages();
        while(this.localAppMessages.size()<1){
            this.localAppMessages = this.sqs.receiveMessage(this.queueURL).getMessages();
        }
    }

    private void deliverJobsToWorkers() {
        for(Message m: this.localAppMessages){
            File f = getFileFromMessage(m);
            List<Job> jobs = parseJobsFromFile(f);
            updateWorkers(jobs.size()/this.workersRatio);
            for(Job job : jobs){
                sendJobMessage(job);
            }
        }
    }

    private void sendJobMessage(Job job) {

    }

    private void updateWorkers(int m) {
        if(m>this.workers.size()){
            this.workers = new ArrayList<>();
            for(int i=0;i<this.workersRatio;i++){
                this.workers.add(createWorker());
            }
        }
    }

    private String createWorker() {
        return "";
    }

    private List<Job> parseJobsFromFile(File f) {
        List<Job> jobs = new ArrayList<>();
        try {
            Scanner myReader = new Scanner(f);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String[] act_url = data.split("\\s+");
                jobs.add(new Job(act_url[0],act_url[1]));
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred reading from the file");
            e.printStackTrace();
        }
        return jobs;
    }

    private File getFileFromMessage(Message m) {
        String fileName = m.getBody();
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

    private void printMessages(){
        for(Message m : this.localAppMessages){
            Map<String,String> att = m.getAttributes();
            System.out.println(att.get(URLS_PACKAGE_KEY));
        }
    }

    public static void main( String[] args )  {
        String bucket_name = args[0];
        String localQ_key = args[1];
        App manager = new App(bucket_name);
        //Loading an existing document
        System.out.println( "Manager is Running" );
        manager.getMessages();
        manager.deliverJobsToWorkers();
        manager.printMessages();

    }

}

