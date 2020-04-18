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
import com.amazonaws.services.sqs.model.SendMessageRequest;

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

    private static final String URLS_PACKAGE_KEY = "urls_package";
    private static final String PATH_TO_JOB_FILE = "pathToJobFile";
    private static final String MAN_TO_WORK_Q = "manToWorkQ";
    private static final String WORK_TO_MAN_Q = "workToManQ";
    private int workersRatio;
    private AmazonSQS sqs;
    private AmazonS3 s3;
    private List<Message> localAppMessages;
    private List<String> workers;
    private String locToManQ,bucketName;
    private String manToLocQ,manToWorkQ,workToManQ;
    private List<Message> workerMessages;

    public App(String bucketName,String localQ_key,String manQ_key) {
        this.bucketName = bucketName;
        this.s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        this.sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        this.queueURL = getLocalAppQueue(URLS_PACKAGE_KEY);
        this.s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
        this.sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
        getLocalAppQueues(localQ_key,manQ_key);
        this.workersRatio = DEFAULT_WORKERS_RATIO;
        createManagerWorkerQs();
    }

    private void createManagerWorkerQs() {
        sqs.createQueue(MAN_TO_WORK_Q);
        sqs.createQueue(WORK_TO_MAN_Q);
        this.manToWorkQ = sqs.getQueueUrl(MAN_TO_WORK_Q).getQueueUrl();
        this.workToManQ = sqs.getQueueUrl(WORK_TO_MAN_Q).getQueueUrl();
    }

    private void getLocalAppQueues(String loc_man_q,String man_loc_q) {
        S3ObjectInputStream s3is = getS3ObjectInputStream(this.bucketName,loc_man_q);
        this.locToManQ =  buildString(s3is);
        s3is = getS3ObjectInputStream(this.bucketName,man_loc_q);
        this.manToLocQ =  buildString(s3is);
        try {
            s3is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    private void getMessagesFromQueues(){
        this.localAppMessages = this.sqs.receiveMessage(this.locToManQ).getMessages();
        this.workerMessages = this.sqs.receiveMessage(this.workToManQ).getMessages();
    }

    private boolean deliverJobsToWorkers() {
        boolean terminate = false;
        for(Message m: this.localAppMessages){
            String fileName = getFileNameFromMap(m.getAttributes());
            //check for termination message
            if(fileName.equals("terminate")) terminate=true;
            int packageId = getIdFromFileName(fileName);
            File f = getFileFromMessage(m,fileName);
            List<Job> jobs = parseJobsFromFile(f,packageId);
            updateWorkers(jobs.size()/this.workersRatio);
            for(Job job : jobs){
                sendJobMessage(job.toString());
            }
        }
        return terminate;
    }

    private void handleWorkersMessages() {

    }

    private int getIdFromFileName(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.indexOf('#')+1));
    }

    private void sendJobMessage(String job) {
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(this.manToWorkQ)
                .withMessageBody(job)
                .withDelaySeconds(5);
        sqs.sendMessage(send_msg_request);
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

    private List<Job> parseJobsFromFile(File f,int packageId) {
        List<Job> jobs = new ArrayList<>();

        try {
            Scanner myReader = new Scanner(f);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String[] act_url = data.split("\\s+");
                jobs.add(new Job(act_url[0],act_url[1], packageId));
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

    private String getFileNameFromMap(Map<String, String> attributes) {
        return attributes.get(PATH_TO_JOB_FILE);
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
        String manQ_key = args[1];
        System.out.println( "action=" + "toImage" +
                ", url='" + "google.co.il" +
                ", packageId=" + 7);
        App manager = new App(bucket_name,localQ_key,manQ_key);
        //Loading an existing document
        System.out.println( "Manager is Running" );
        boolean terminate = false;
        while(!terminate) {
            manager.getMessagesFromQueues();
            manager.handleWorkersMessages();
            terminate = manager.deliverJobsToWorkers();
        }

    }

}

