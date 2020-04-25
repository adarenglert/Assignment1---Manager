package Local;

import java.io.IOException;


import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import Operator.Storage;
import Operator.Queue;
import Operator.Machine;

import java.util.*;


public class App {

    //#############################################################
    // Class's Members
    //#############################################################

    private static final String ACCESS_KEY = "AKIA3W3ZEDH6VT4MVK6E";
    private static final String SECRET_KEY = "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";
    private static final String WORKER_USER_DATA = "worker_user_data";
//    private static final String ACCESS_KEY = "AKIAJCAW3R5VDDEVSWJQ";
//    private static final String SECRET_KEY = "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";
private static final int WAIT_TIME_SECONDS = 3;
    private static final String MANAGER_Q_KEY = "loc_man_key"; // Key to the the name of manager to local queue.
    private static final String LOCAL_Q_KEY = "man_loc_key"; // Key to the the name of local to manager queue.
    private static final String KEY_TO_JOB_FILE = "keyToJobFile"; // Key to the input file.
    private static final String ID_KEY = "idKey"; // Key to Local App ID.
    private static final String LOCAL_QUEUE_NAME = "localq";
    private static final String MANAGER_QUEUE_NAME = "manq";
    private static final String BUCKET_NAME = "disthw1bucket";
    private static final String UBUNTU_JAVA_11_AMI = "ami-0bec39ebceaa749f0";
    private static final String MANAGER_INST_ID = "manager_inst_id";
    private final Storage storage;
    private final Queue localQ;
    private final Queue managerQ;
    private final boolean managerRunning;
    private final Machine machine;


    private String workersRatio;
    private int packageId;
    private SqsClient sqs;
    private S3Client s3;
    private String inputFile, outputFile;

    private Ec2Client ec2;
    private String managerInstId;


    //Constructor of class App

    public App(String inputFile, String outputFile, String n) {
//        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
//                ACCESS_KEY,
//                SECRET_KEY
//        );
        this.s3 = S3Client.builder().region(Region.US_EAST_1)
        //        .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        this.sqs = SqsClient.builder().region(Region.US_EAST_1)
          //      .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        this.ec2 = Ec2Client.builder()
                .region(Region.US_EAST_1)
            //    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();

        this.workersRatio = n;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.storage = new Storage("disthw1bucket", getS3());
        this.localQ = new Queue(getLocalQueueName() + "-" + getPackageId(), getSqs());
        this.localQ.createQueue();
        this.managerQ = new Queue(getManagerQueueName(), getSqs());
        this.managerRunning = storage.isObjectExist(ID_KEY);
        this.managerInstId = "";
        this.machine = new Machine(ec2,UBUNTU_JAVA_11_AMI);
        storage.uploadFile(WORKER_USER_DATA,"loadcreds.sh");
    }

    //############################################
    //Private Functions
    //###########################################

    //Getters and Setters


    public String getInput_file() {
        return this.inputFile;
    }

    public String getOutput_file() {
        return this.outputFile;
    }

    public String getWorkersRatio() {
        return this.workersRatio;
    }

    public SqsClient getSqs() {
        return this.sqs;
    }

    public S3Client getS3() {
        return this.s3;
    }

    public String getManagerQKey() {
        return this.MANAGER_Q_KEY;
    }

    public String getLocalQKey() {
        return this.LOCAL_Q_KEY;
    }

    public String getKeyToJobFile() {
        return this.KEY_TO_JOB_FILE;
    }

    public String getIdKey() { return this.ID_KEY; }

    public String getLocalQueueName() { return this.LOCAL_QUEUE_NAME; }

    private String getManagerInstId() { return managerInstId; }

    public String getManagerQueueName() {
        return this.MANAGER_QUEUE_NAME;
    }

    public String getBucketName() {
        return this.BUCKET_NAME;
    }

    public int getPackageId() {
        return this.packageId;
    }

    public void setPackageId(int packageId) {
        this.packageId = packageId;
    }

    //Operating Functions

    public void sendPackage(boolean terminate) {
        storage.uploadFile(getKeyToJobFile() + "#" + getPackageId(), inputFile);
        storage.uploadName(getLocalQKey() + "#" + getPackageId(), getLocalQueueName() + "-" + getPackageId());
        managerQ.sendMessage(getKeyToJobFile() + "#" + getPackageId());
        if(terminate){
            managerQ.sendMessage("terminate#"+packageId);
        }
    }

    public void initId(Storage storage) {
        String StringId = storage.getString(ID_KEY);
        this.packageId = Integer.parseInt(StringId) + 1;
        storage.uploadName(ID_KEY,String.valueOf(this.packageId));
    }

    private void getManager() {
        if (!managerRunning) {
            setPackageId(0);
            storage.uploadName(ID_KEY,"0");
            managerQ.createQueue();
            storage.uploadName(getManagerQKey(), getManagerQueueName());

            try {
                managerInstId =  machine.createInstance("Manager",Integer.parseInt(workersRatio));
                storage.uploadName(MANAGER_INST_ID,managerInstId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            initId(storage);
            managerInstId = storage.getString(MANAGER_INST_ID);
        }
    }


    //##############################################
    // Main Function
    //##############################################

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];
        String tasks_num = args[2];
        boolean terminate = false;
        if(args.length>3)
            terminate = args[4].equals("terminate");

        App local = new App(inputFile, outputFile, tasks_num);
        local.getManager();
        local.sendPackage(terminate);
        boolean gotSummary=false;
        boolean gotTerminate=!terminate;

        while(!(gotSummary & gotTerminate)){
            List<Message> msgs = local.localQ.receiveMessages(1,WAIT_TIME_SECONDS);
            if(!msgs.isEmpty()) {
                Message m = msgs.get(0);
                switch (m.body()){
                    case "summary.txt":
                        gotSummary = true;
                        local.storage.getFile("summary#" + local.getPackageId(), outputFile);
                        break;
                    case "terminate":
                        gotTerminate = true;
                        gotSummary = true;
                        local.machine.stopInstance(local.getManagerInstId());
                        break;
                }
                local.localQ.deleteMessage(m);
            }


        }
        System.out.println("Local App Finishedddd!!!!");
//TODO convert output to html
        //TODO All APPs Error catching support
    }


}



//TODO counter down when we finish.
//TODO add catch for a specific exception for object's key not to be found.



