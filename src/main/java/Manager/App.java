package Manager;

import Operator.Queue;
import Operator.Storage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class App {
    private static final int DEFAULT_WORKERS_RATIO = 100;
    private static final String MAN_TO_WORK_Q_NAME = "manToWorkQ";
    private static final String WORK_TO_MAN_Q_NAME = "workToManQ";
    private static final String MAN_TO_WORK_Q_KEY = "manToWorkQ_key";
    private static final String WORK_TO_MAN_Q_KEY = "workToManQ_key";
    private static final int NUM_OF_MESSAGES = 5;
    private static final String ACCESS_KEY = "AKIAJ3VHZVBVKAG73NFQ";
    private static final String SECRET_KEY = "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";
    private final Storage storage;
    private final SqsClient sqs;
    private final HashMap<Integer, File> results;
    private HashMap<Integer, List<Job>> tasks;
    private int workersRatio;
    private List<Worker.App> workers;
    private Queue locToManQ;
    private Queue manToLocQ;
    private Queue manToWorkQ;
    private Queue workToManQ;
    private boolean gotTerminate;
    private boolean allDone;

    public App(String bucketName,String localQ_key,String manQ_key) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                ACCESS_KEY,
                SECRET_KEY
        );
        this.storage = new Storage(bucketName, S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build());

        this.sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();

        getLocalAppQueues(localQ_key,manQ_key);
        createManagerWorkerQs();
        this.workersRatio = DEFAULT_WORKERS_RATIO;
        this.workers = new ArrayList<>();
        this.allDone = true;
        this.tasks = new HashMap<>();
        this.results = new HashMap<>();
    }

    private void createManagerWorkerQs() {
        this.manToWorkQ = new Queue(MAN_TO_WORK_Q_NAME,sqs);
        this.workToManQ = new Queue(WORK_TO_MAN_Q_NAME,sqs);
        this.manToWorkQ.createQueue();
        this.workToManQ.createQueue();
        storage.uploadName(MAN_TO_WORK_Q_KEY,workToManQ.getName());
        storage.uploadName(WORK_TO_MAN_Q_KEY,manToWorkQ.getName());
    }

    private Worker.App createWorker() {
        Worker.App w = new Worker.App(storage.getName(),MAN_TO_WORK_Q_KEY,WORK_TO_MAN_Q_KEY);
        return w;
    }

    private void getLocalAppQueues(String loc_man_key,String man_loc_key) {
        String loc_man_q_name = this.storage.getString(loc_man_key);
        String man_loc_q_name = this.storage.getString(man_loc_key);
        this.locToManQ = new Queue(loc_man_q_name,sqs);
        this.manToLocQ = new Queue(man_loc_q_name,sqs);
    }

    private void setTerminate(String content){this.gotTerminate = content.equals("terminate");}

    private void deliverJobsToWorkers() throws FileNotFoundException {
        List<Message> msgs = this.locToManQ.receiveMessages(NUM_OF_MESSAGES);
        for(Message m: msgs){
            String fileName = m.body();
            //check for termination message
            setTerminate(fileName);
            if(gotTerminate) break;
            this.allDone = false;
            int packageId = getIdFromFileName(fileName);
            storage.getFile(fileName,fileName);
            List<Job> jobs = parseJobsFromFile(new File(fileName),packageId);
            tasks.put(packageId,jobs);
           //updateWorkers(jobs.size()/this.workersRatio);
            for(Job job : jobs){
                this.manToWorkQ.sendMessage(job.toString());
                break; //TODO
            }
//            locToManQ.deleteMessage(m);
        }
    }

    private void handleWorkersMessages() throws IOException {
        List<Message> msgs = this.workToManQ.receiveMessages(NUM_OF_MESSAGES);
        for(Message m: msgs){
            Job j = Job.buildFromMessage(m.body());
            int packageId = j.getPackageId();
            List<Job> l = tasks.get(packageId);
            addResult(j);
            findAndRemove(j,l);
            tasks.put(packageId,l);
        }
        sendResults();
    }

    private void findAndRemove(Job j, List<Job> l) {
        int i=0;
        for(Job jo : l){
            if(jo.getUrl().equals(j.getUrl()))
                break;
            i++;
        }
        if(i>=0 & i< l.size())
            l.remove(i);
    }

    private void addResult(Job j) throws IOException {
        int packageId = j.getPackageId();
        File f = results.get(packageId);
        if(f==null){
            String fname = "output#"+packageId;
            f = new File(fname);
            results.put(packageId,f);
        }
        BufferedWriter output = new BufferedWriter(new FileWriter(f.getName(), true));
        output.append(j.getAction()+':'+" "+j.getUrl()+" "+ j.getOutputUrl()+'\n');
        output.close();
    }

    private void sendResults() {
        for(Integer packageid : tasks.keySet()){
            List<Job> jobs = tasks.get(packageid);
            if(jobs.isEmpty()) {
                String key = "summary";
                storage.uploadFile(key,results.get(packageid).getPath());
                manToLocQ.sendMessage(key);
                tasks.remove(packageid);
                if(tasks.isEmpty()) allDone=true;
            }
        }
    }

    private int getIdFromFileName(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.indexOf('#')+1));
    }

    private void updateWorkers(int m) {
        if(m>this.workers.size()){
            this.workers = new ArrayList<>();
            for(int i=0;i<1;i++){
                this.workers.add(createWorker());
            }
        }
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
        String manQ_key = args[2];
        App manager = new App(bucket_name,localQ_key,manQ_key);
        //Loading an existing document
        System.out.println( "Manager is Running" );
        while(!(manager.gotTerminate & manager.allDone)) {
            try {
                manager.deliverJobsToWorkers();
                while(true) manager.handleWorkersMessages();
                //TimeUnit.SECONDS.sleep(3);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } //catch (InterruptedException e) {
                //e.printStackTrace();
        //    }
        catch (NullPointerException e) {
                e.printStackTrace();
            }
        }

    }

}

