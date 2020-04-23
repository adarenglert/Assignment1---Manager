package Local;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Random;


import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;


import java.io.*;
import java.util.*;
import java.util.function.Consumer;



public class App {

    //#############################################################
    // Class's Members
    //#############################################################

//    private static final String ACCESS_KEY = "AKIAJ3VHZVBVKAG73NFQ";
//    private static final String SECRET_KEY = "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";

    private static final String ACCESS_KEY = "AKIAJCAW3R5VDDEVSWJQ";
    private static final String SECRET_KEY = "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";

    private static final String MANAGER_Q_KEY = "loc_man_key"; // Key to the the name of manager to local queue.
    private static final String LOCAL_Q_KEY = "man_loc_key"; // Key to the the name of local to manager queue.
    private static final String KEY_TO_JOB_FILE = "keyToJobFile"; // Key to the input file.
    private static final String ID_KEY = "idKey"; // Key to Local App ID.
    private static final String LOCAL_QUEUE_NAME = "localq";
    private static final String MANAGER_QUEUE_NAME = "manq";
    private static final String BUCKET_NAME = "disthw1bucket";


    private int workersRatio, packageId;
    private SqsClient sqs;
    private S3Client s3;
    private String inputFile, outputFile;

    private Ec2Client ec2;


    //Constructor of class App

    public App(String inputFile, String outputFile, int n) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                this.ACCESS_KEY,
                this.SECRET_KEY
        );
        this.s3 = S3Client.builder().region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        this.sqs = SqsClient.builder().region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        this.ec2 = Ec2Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        this.workersRatio = n;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
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

    public int getWorkersRatio() {
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

    public String getIdKey() {
        return this.ID_KEY;
    }

    public String getLocalQueueName() {
        return this.LOCAL_QUEUE_NAME;
    }

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

    public void initManager() {

    }

    public void initId(Storage storage) {
        String StringId = storage.getString(ID_KEY);
        this.packageId = Integer.parseInt(StringId) + 1;
    }


    //##############################################
    // Main Function
    //##############################################

    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];
        String tasks_num = args[2];
        int tasks_per_worker = Integer.parseInt(tasks_num);
        App local = new App(inputFile, outputFile, tasks_per_worker);

        System.out.println("input: " + inputFile + "\noutput: " + outputFile + "\nnumber of workers: " + tasks_per_worker);

        Storage storage = new Storage("disthw1ec2test", local.getS3());


        boolean isExist = storage.isObjectExist(ID_KEY);

        Queue managerQ = new Queue(local.getManagerQueueName(), local.getSqs());

        if (!isExist) {
            local.setPackageId(0);
            managerQ.createQueue();
        } else {
            local.initId(storage);
        }


        storage.uploadName(local.getIdKey(), String.valueOf(local.getPackageId()));

        storage.uploadName(local.getManagerQKey(), local.getManagerQueueName());

        storage.uploadFile(local.getKeyToJobFile() + "#" + local.getPackageId(), inputFile);


        Queue localQ = new Queue(local.getLocalQueueName() + "-" + local.getPackageId(), local.getSqs());
        localQ.createQueue();
        storage.uploadName(local.getLocalQKey() + "#" + local.getPackageId(), local.getLocalQueueName() + "-" + local.getPackageId());

        managerQ.sendMessage(local.getKeyToJobFile() + "#" + local.getPackageId());

        storage.uploadFile("Hello World", "App-1.0.jar");

        Machine machine = new Machine(local.ec2,"App-1.0", "ami-0915e09cc7ceee3ab", storage.getURL("Hello World"));

        machine.createInstance();


        System.out.println("Local App is Running");


    }
}



//TODO counter down when we finish.
//TODO add catch for a specific exception for object's key not to be found.



