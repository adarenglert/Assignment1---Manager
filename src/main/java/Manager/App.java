package Manager;

import Operator.Queue;
import Operator.Storage;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

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
    private static final int NUM_OF_MESSAGES = 5;
    private final Storage storage;
    private final SqsClient sqs;
    private int workersRatio;
    private List<Message> localAppMessages;
    private List<Worker.App> workers;
    private Queue locToManQ;
    private String bucketName;
    private Queue manToLocQ;
    private Queue manToWorkQ;
    private Queue workToManQ;
    private List<Message> workerMessages;

    public App(String bucketName,String localQ_key,String manQ_key) {
        this.sqs = SqsClient.builder()
                .build();
        this.storage = new Storage(bucketName, S3Client.builder().build());
        getLocalAppQueues(localQ_key,manQ_key);
        createManagerWorkerQs();
        this.workersRatio = DEFAULT_WORKERS_RATIO;
    }

    private void createManagerWorkerQs() {
        this.manToWorkQ = new Queue(MAN_TO_WORK_Q,sqs);
        this.workToManQ = new Queue(WORK_TO_MAN_Q,sqs);
        this.manToWorkQ.createQueue();
        this.workToManQ.createQueue();
    }

    private void getLocalAppQueues(String loc_man_key,String man_loc_key) {
        String loc_man_q_name = this.storage.getString(loc_man_key);
        String man_loc_q_name = this.storage.getString(man_loc_key);
        this.locToManQ = new Queue(loc_man_q_name,sqs);
        this.manToLocQ = new Queue(man_loc_q_name,sqs);
    }

    private boolean deliverJobsToWorkers() throws FileNotFoundException {
        boolean terminate = false;
        for(Message m: this.localAppMessages){
            String fileName = m.body();
            //check for termination message
            if(fileName.equals("terminate")) terminate=true;
            int packageId = getIdFromFileName(fileName);
            storage.getFile(fileName,fileName);
            List<Job> jobs = parseJobsFromFile(new File(fileName),packageId);
            updateWorkers(jobs.size()/this.workersRatio);
            for(Job job : jobs){
                this.manToWorkQ.sendMessage(job.toString());
            }
        }
        return terminate;
    }

    private void handleWorkersMessages() {

    }

    private int getIdFromFileName(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.indexOf('#')+1));
    }

    private void updateWorkers(int m) {
        if(m>this.workers.size()){
            this.workers = new ArrayList<>();
            for(int i=0;i<this.workersRatio;i++){
                this.workers.add(createWorker());
            }
        }
    }

    private Worker.App createWorker() {
        Worker.App w = new Worker.App(bucketName,workToManQ.getName(),manToWorkQ.getName());
        w.run();
        return w;
    }

    private List<Job> parseJobsFromFile(File f,int packageId) throws FileNotFoundException {
        List<Job> jobs = new ArrayList<>();
            Scanner myReader = new Scanner(f);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String[] act_url = data.split("\\s+");
                jobs.add(new Job(act_url[0],act_url[1], packageId));
            }
            myReader.close();
        return jobs;
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

