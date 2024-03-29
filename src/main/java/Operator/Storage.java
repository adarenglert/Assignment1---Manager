package Operator;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import java.util.ListIterator;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.s3.S3Utilities;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.ResponseTransformer;


public class Storage {

    //###########################
    // Class's fields
    //###########################

    private String bucketName;
    private S3Client s3;


    //  Constructor

    public Storage(String bucketName, S3Client s3){
        this.bucketName = bucketName;
        this.s3 = s3;
    }

    //#############################
    // Class's functions
    //#############################

    public String getName(){
        return this.bucketName;
    }

    public S3Client getS3(){
        return this.s3;
    }


    public void createBucket(){
        CreateBucketRequest createBucketRequest = CreateBucketRequest
                .builder()
                .bucket(this.bucketName)
                .createBucketConfiguration(CreateBucketConfiguration.builder()
                        .build())
                .build();
        s3.createBucket(createBucketRequest);
    }

    //Added Functions


    public boolean isObjectExist(String key){
        try {
            ResponseBytes gdido = s3.getObject(GetObjectRequest.builder().bucket(this.bucketName).key(key).build(),
                    ResponseTransformer.toBytes());
            String ret = gdido.asString(Charset.defaultCharset());
            return true;
        }
        catch(NoSuchKeyException e){
            System.out.println(e.getMessage());
            return false;
        }
    }

    public void uploadName(String key,String value) {
        try {
            s3.putObject(PutObjectRequest.builder().bucket(this.bucketName).key(key)
                            .build(),
                    RequestBody.fromString(value));
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }

    }

    public void uploadFile(String key, String path){
        File file = new File(path);

        try {
            s3.putObject(PutObjectRequest.builder().bucket(this.bucketName).key(key)
                            .build(),
                    RequestBody.fromFile(file));
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }

    }

    public String getURL(String key){
        S3Utilities utilities = s3.utilities();
        GetUrlRequest request = GetUrlRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        URL url = utilities.getUrl(request);
        return url.toString();
    }

    public String getString(String key){
        try {
            ResponseBytes gdido = s3.getObject(GetObjectRequest.builder().bucket(this.bucketName).key(key).build(),
                    ResponseTransformer.toBytes());
            String ret = gdido.asString(Charset.defaultCharset());
            return ret;
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }

        return null;
    }
    public void getFile(String key, String fileName){
        File f = new File(fileName);
        if(f.exists()) f.delete();
        try {
            s3.getObject(GetObjectRequest.builder().bucket(this.bucketName).key(key)
                            .build(),
                    ResponseTransformer.toFile(Paths.get(fileName)));
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void deleteObject(String key){
        try {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(this.bucketName).key(key).build());
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void removeObjects(String[] doNotRemove) {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                S3Object myValue = (S3Object) iterVals.next();
                boolean remove = containsString(doNotRemove,myValue.key());
                if(remove)
                     deleteObject(myValue.key());
            }
    }

    private boolean containsString(String[] doNotRemove, String key) {
        for(String s: doNotRemove){
            if(s.equals(key)) return false;
        }
        return true;
    }
}