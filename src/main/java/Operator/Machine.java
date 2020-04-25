package Operator;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

public class Machine {
    private static final String MY_AMI = "ami-06132e0dc01051fed";
    private static final String WORK_TO_MAN_Q_KEY = "workToManQ_key";
    private static final String MAN_TO_WORK_Q_KEY = "manToWorkQ_key";
    private Ec2Client ec2;
    private String ami;
    private String jarUrl;


    public Machine(Ec2Client ec2, String ami){
        this.ec2 = ec2;
        this.ami = ami;
    }


    public String createInstance(String module, int n) throws IOException {
        String userData = "";
        if(module.equals("Manager")) {
            userData = new String(Files.readAllBytes(Paths.get("loadcredsManager.sh")));
            userData += " "+module + ".jar " + "disthw1bucket loc_man_key " + n + '\n';
        }
        else{
            userData = new String(Files.readAllBytes(Paths.get("loadcredsWorker.sh")));
            userData += " "+module + ".jar " +  MAN_TO_WORK_Q_KEY + " " + WORK_TO_MAN_Q_KEY + '\n';
        }
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
               // .imageId(this.ami)
                .imageId(MY_AMI)
                .instanceType(InstanceType.T2_MICRO)
                .userData(Base64.getEncoder().encodeToString((userData).getBytes()))
                .maxCount(1)
                .minCount(1)
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        String dns = response.instances().get(0).publicDnsName();

        Tag tag = Tag.builder()
                .key("Name")
                .value(module)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.println(
                    "Successfully started EC2 instance "+instanceId+" based on AMI " +
                      this.ami);

        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return instanceId;
    }

    public void stopInstance(String instanceId) {
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.stopInstances(request);
    }

}
