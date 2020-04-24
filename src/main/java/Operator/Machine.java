package Operator;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;

import java.util.Base64;

public class Machine {

    private static final String script =  "#!/bin/bash\n"+
            "wget";

    private Ec2Client ec2;
    private String module;
    private String ami;
    private String jarUrl;


    public Machine(Ec2Client ec2, String module, String ami, String jarUrl){
        this.ec2 = ec2;
        this.module = module;
        this.ami = ami;
        this.jarUrl = jarUrl;
    }


    public void createInstance(){

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(this.ami)
                .instanceType(InstanceType.T1_MICRO)
                .userData(Base64.getEncoder().encodeToString((this.script + this.jarUrl + "\n" + "java -jar " +
                        this.module + ".jar\n").getBytes()))
                .maxCount(1)
                .minCount(1)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("Name")
                .value("Value")
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 instance %s based on AMI %s",
                    instanceId, this.ami);

        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[ec2.java2.create_instance.main]
        System.out.println("Done!");
    }

}
