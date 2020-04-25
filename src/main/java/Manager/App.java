package Manager;

import Operator.Machine;
import Operator.Queue;
import Operator.Storage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class App {
    public static Storage debug_storage;
    private static final int DEFAULT_WORKERS_RATIO = 100;
    private static final String MAN_TO_WORK_Q_NAME = "manToWorkQ";
    private static final String WORK_TO_MAN_Q_NAME = "workToManQ";
    private static final String WORK_TO_MAN_Q_KEY = "workToManQ_key";
    private static final String MAN_TO_WORK_Q_KEY = "manToWorkQ_key";
    private static final String RESULTS_BUCKET = "disthw1results";
    private static final int NUM_OF_MESSAGES = 5;
    private static final String UBUNTU_JAVA_11_AMI = "ami-0bec39ebceaa749f0";
    private static final String ACCESS_KEY = "AKIAJ3VHZVBVKAG73NFQ";
    private static final String SECRET_KEY = "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";
    private static final String WORKER_USER_DATA = "worker_user_data";
    private static final String DEBUG_Q = "gadid_debug_queue";
    private static final int WAIT_TIME_SECONDS = 3;
    private static final int MAX_EC2_INSTS = 3;
    private final Storage storage;
    private final SqsClient sqs;
    private final HashMap<Integer, File> results;
    private final Ec2Client ec2;
    private final Machine machine;
    private HashMap<Integer, List<Job>> tasks;
    private int workersRatio;
    private List<String> workersCount;
    private Queue locToManQ;
    private HashMap<Integer,Queue> manToLocQ;
    private Queue manToWorkQ;
    private Queue workToManQ;
    private boolean gotTerminate;
    private boolean allDone;
    private Queue debugQ;
    private Integer localTermId;

    public App(String bucketName, String localQ_key, String manQ_key, int ratio) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                ACCESS_KEY,
                SECRET_KEY
        );
        this.storage = new Storage(bucketName, S3Client.builder()
                .region(Region.US_EAST_1)
           //     .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build());

        this.sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
             //   .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();

        this.ec2 = Ec2Client.builder()
                .region(Region.US_EAST_1)
               // .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();

        this.machine = new Machine(ec2,UBUNTU_JAVA_11_AMI);

        this.workersRatio = ratio;
        this.workersCount = new ArrayList<>();
        this.allDone = true;
        this.tasks = new HashMap<>();
        this.results = new HashMap<>();
        this.manToLocQ = new HashMap<>();
        this.localTermId = -1;
        getLocalAppQueues(localQ_key,manQ_key);
        createManagerWorkerQs();
        setWorkerUserData();
        //DEBUG
//        createDebugQueue();
    }

    private void createDebugQueue() {
        this.debugQ = new Queue(DEBUG_Q,sqs);
        this.debugQ.createQueue();
    }

    private void createManagerWorkerQs() {
        this.manToWorkQ = new Queue(MAN_TO_WORK_Q_NAME,sqs);
        this.workToManQ = new Queue(WORK_TO_MAN_Q_NAME,sqs);
        this.manToWorkQ.createQueue();
        this.workToManQ.createQueue();
        storage.uploadName(MAN_TO_WORK_Q_KEY,workToManQ.getName());
        storage.uploadName(WORK_TO_MAN_Q_KEY,manToWorkQ.getName());
    }

    private void getLocalAppQueues(String loc_man_key,String man_loc_key) {
        String loc_man_q_name = this.storage.getString(loc_man_key);
        String man_loc_q_name = this.storage.getString(man_loc_key);
        this.locToManQ = new Queue(loc_man_q_name,sqs);
        this.addLocalQueue(0,new Queue(man_loc_q_name,sqs));
    }

    private void addLocalQueue(int packageId, Queue q) {
        this.manToLocQ.put(packageId,q);
    }

    private void setTerminate(String content){this.gotTerminate = content.equals("terminate");}

    private void deliverJobsToWorkers() throws IOException {
        List<Message> msgs = this.locToManQ.receiveMessages(NUM_OF_MESSAGES,WAIT_TIME_SECONDS);
        for(Message m: msgs){
            debug_storage.uploadName("got message form local","");
            String fileName = m.body();
            //check for termination message
            int packageId = getIdFromFileName(fileName);
            setTerminate(fileName);
            if(gotTerminate) {
                debug_storage.uploadName("got terminate from local","");
                this.localTermId = packageId;
                break;
            }
            this.allDone = false;
            storage.getFile(fileName,fileName);
            List<Job> jobs = parseJobsFromFile(new File(fileName),packageId);
            tasks.put(packageId,jobs);
           updateWorkers(jobs.size()/this.workersRatio);
           //TODO - change to threads
            for(Job job : jobs){
                this.manToWorkQ.sendMessage(job.toString());
            }
            locToManQ.deleteMessage(m);
        }
    }

    private void handleWorkersMessages() throws IOException {
        List<Message> msgs = this.workToManQ.receiveMessages(NUM_OF_MESSAGES,WAIT_TIME_SECONDS);
        for(Message m: msgs){
            debug_storage.uploadName("got message from worker","");
            Job j = Job.buildFromMessage(m.body());
            int packageId = j.getPackageId();
            List<Job> l = tasks.get(packageId);
            addResult(j);
            findAndRemove(j,l);
            tasks.put(packageId,l);
            workToManQ.deleteMessage(m);
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
            String fname = "summary#"+packageId;
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
                debug_storage.uploadName("summary file sent to local","");
                String key = "summary.txt";
                File f = results.get(packageid);
                storage.uploadFile(f.getName(),f.getPath());
                manToLocQ.get(packageid).sendMessage(key);
                tasks.remove(packageid);
                manToLocQ.remove(packageid);
                if(tasks.isEmpty()) allDone=true;
            }
        }
    }

    private int getIdFromFileName(String fileName) {
        return Integer.parseInt(fileName.substring(fileName.indexOf('#')+1));
    }

    private void updateWorkers(int m) throws IOException {
        if(m>this.workersCount.size()){
            if(m>MAX_EC2_INSTS) m = MAX_EC2_INSTS;
            for(int i=0;i<m;i++){
                String instanceId = this.machine.createInstance("Worker",-1);
                workersCount.add(instanceId);
            }
        }
    }

    public void setWorkerUserData(){
        storage.getFile(WORKER_USER_DATA,"loadcreds.sh");
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

    private void closeAll() {
        for(String instanceId : workersCount)
            this.machine.stopInstance(instanceId);
        manToLocQ.get(this.localTermId).sendMessage("terminate");
        debug_storage.uploadName("manager finished","");
    }

    public static void main( String[] args )  {

        debug_storage = new Storage(RESULTS_BUCKET,S3Client.builder()
                .region(Region.US_EAST_1)
                .build());
        String bucket_name = args[0];
        String localQ_key = args[1];
        String manQ_key = args[2];
        int ratio = Integer.parseInt(args[3]);

        App manager = new App(bucket_name,localQ_key,manQ_key,ratio);

        while(!(manager.gotTerminate & manager.allDone)) {
            try {
                debug_storage.uploadName("manager loop started","");
                manager.deliverJobsToWorkers();
                manager.handleWorkersMessages();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                debug_storage.uploadName("Error! "+e.getCause(), e.getMessage());
            } catch (IOException e) {
                debug_storage.uploadName("Error! "+e.getCause(), e.getMessage());
                e.printStackTrace();
            }
        catch (NullPointerException e) {
                e.printStackTrace();
            debug_storage.uploadName("Error! "+e.getCause(), e.getMessage());
            }
        }
        manager.closeAll();
    }


}

