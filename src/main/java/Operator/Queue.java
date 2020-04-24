package Operator;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;


public class Queue {

    //##############################
    // Class's Fields
    //##############################

    private String queueName;
    private SqsClient sqs;
    private String queueUrl;

    //##############################
    // Constructor
    //##############################

    public Queue(String queueName, SqsClient sqs){
        this.queueName = queueName;
        this.sqs = sqs;
    }

    //##############################
    // CLass's Functions
    //##############################

    public String getName(){
        return queueName;
    }

    public SqsClient getSqs(){
        return this.sqs;
    }

    public String getUrl(){
        if(this.queueUrl == null){
            GetQueueUrlResponse getQueueUrlResponse =
                    sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName(this.queueName).build());
            this.queueUrl = getQueueUrlResponse.queueUrl();
        }
        return this.queueUrl;
    }

    public void createQueue(){
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(this.queueName).build();
        sqs.createQueue(createQueueRequest);
    }

    public void deleteQueue(){
        DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(this.getUrl()).build();
        sqs.deleteQueue(deleteQueueRequest);
    }

    public void sendMessage(String message){
        this.sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(this.getUrl())
                .messageBody(message)
                .delaySeconds(10)
                .build());
    }

//    public void sendJob(Job job){
//        this.sqs.sendMessage(SendMessageRequest.builder()
//                .queueUrl(this.getUrl())
//                .messageBody("Job Message")
//                .delaySeconds(10)
//                .build());
//    }
//
//    public String serializeMessage(Job job){
//        String encodedMessage;
//        byte[] data = SerializationUtils.serialize(job);
//        Job gad = SerializationUtils.deserialize(data);
//        encodedMessage = new String(data);
//        byte[] data2 = encodedMessage.getBytes(StandardCharsets.UTF_8);
//        byte[] data3 = StandardCharsets.UTF_8.encode(encodedMessage).array();
//        if(data == data2){
//            System.out.println("They are equal");
//        }
//        return encodedMessage;
//    }
//
//    public Job deserializeMessage(String encodedMessage){
//        System.out.println("The encoded: " + encodedMessage);
//        byte[] data = null;
//        data = encodedMessage.getBytes(StandardCharsets.UTF_8);
//        Job job = null;
//        try {
//            job = SerializationUtils.deserialize(data);
//        }
//        catch(Exception e) {
//            System.out.println("The string is wrong " + e.getMessage());
//        }
//        return job;
//    }

    public Message receiveMessage(){
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(this.getUrl())
                .maxNumberOfMessages(1)
                .build();
        List<Message> messages = this.sqs.receiveMessage(receiveMessageRequest).messages();
        return messages.get(0);
    }

    public List<Message> receiveMessages(int n,int sec){
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(this.getUrl())
                .maxNumberOfMessages(n)
                .waitTimeSeconds(sec)
                .build();
        List<Message> messages = this.sqs.receiveMessage(receiveMessageRequest).messages();
        return messages;
    }

    public void deleteMessage(Message message){
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        this.sqs.deleteMessage(deleteMessageRequest);
    }


}




//TODO check if the queue exist before creating.
//TODO question mark on delete queue.