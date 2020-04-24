package Worker;

import Manager.Job;
import Operator.Queue;
import Operator.Storage;
import org.apache.pdfbox.multipdf.PageExtractor;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class App
{
    private static final String RESULTS_BUCKET = "disthw1results";
    private static final String MAIN_BUCKET = "disthw1bucket";
    private static final int WAIT_TIME_SECONDS = 5;
    final private SqsClient sqs;
    private final Queue work_manQ;
    private final Queue man_workQ;
    private final Storage storage;
    private static final String ACCESS_KEY = "AKIAJ3VHZVBVKAG73NFQ";
    private static final String SECRET_KEY = "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";
    private final Storage storage_results;
    private List<Message> messages;

    public App(String worker_man_key,String man_worker_key) {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                ACCESS_KEY,
                SECRET_KEY
        );
        this.storage_results = new Storage(RESULTS_BUCKET, S3Client.builder()
                .region(Region.US_EAST_1)
//                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build());

        this.storage = new Storage(MAIN_BUCKET, S3Client.builder()
                .region(Region.US_EAST_1)
//                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build());

        this.sqs = SqsClient.builder()
                .region(Region.US_EAST_1)
//                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
        String work_man_q_name = this.storage.getString(worker_man_key);
        String man_work_q_name = this.storage.getString(man_worker_key);
        this.work_manQ = new Queue(work_man_q_name,sqs);
        this.man_workQ = new Queue(man_work_q_name,sqs);
    }

    public static void main( String[] args ){
        App worker = new App(args[0],args[1]);
        while(true){
            try {
                worker.storage_results.uploadName("manager loop started","");
                List<Message> msgs = worker.man_workQ.receiveMessages(1,WAIT_TIME_SECONDS);
                if(!msgs.isEmpty()) {
                    Message m = msgs.get(0);
                    worker.storage_results.uploadName("worker got "+m.body()+ " messages from manager","");
                    Job job = Job.buildFromMessage(m.body());
                    worker.storage_results.uploadName("Job was built","");
                    String filename = worker.extractFileNameFromURL(job.getUrl());
                    worker.storage_results.uploadName("file name extracted","");
                    worker.downloadPDF(job.getUrl(), filename);
                    worker.storage_results.uploadName("pdf was downloaded","");
                    String outputFile = worker.performOp(job.getAction(), filename);
                    worker.storage_results.uploadName("op performed","");
                    worker.storage.uploadFile(outputFile, outputFile);
                    worker.storage_results.uploadName("file was uploaded","");
                    job.setOutputUrl(worker.storage.getURL(outputFile));
                    worker.work_manQ.sendMessage(job.toString());
                    worker.storage_results.uploadName("message was sent to manager","");
                    worker.man_workQ.deleteMessage(m);
                }
                else{
                    worker.storage_results.uploadName("worker got "+msgs.size()+ " messages from manager","");
                }
            }catch (IOException e) {
                worker.storage_results.uploadName("Error! "+e.getCause(), e.getMessage());
                e.printStackTrace();
            }
          }
    }


    private String extractFileNameFromURL(String input) {
        int i = input.lastIndexOf('/')+1;
        return input.substring(i);
    }
    /**
     * download each pdf to your local machine
     *
     */
    private void downloadPDF(String url, String fname) {
        URL pdfUrl = null;
        try {
            pdfUrl = new URL(url);
            URLConnection urlc = pdfUrl.openConnection();
            urlc.setRequestProperty("User-Agent", "Mozilla 5.0 (Windows; U; "
                    + "Windows NT 5.1; en-US; rv:1.8.0.11) ");
            try (BufferedInputStream in = new BufferedInputStream(urlc.getInputStream());
                 FileOutputStream fileOutputStream = new FileOutputStream(fname)) {
                byte dataBuffer[] = new byte[1024*8];
                int bytesRead;
                while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
                    fileOutputStream.write(dataBuffer, 0, bytesRead);
                }
            } catch (IOException e) {
                storage_results.uploadName("Error! "+e.getCause(), e.getMessage());
               e.printStackTrace();
            }
        } catch (MalformedURLException e) {
            storage_results.uploadName("Error! "+e.getCause(), e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            storage_results.uploadName("Error! "+e.getCause(), e.getMessage());
            e.printStackTrace();
        }
    }
    private String toImage(String inputFile) throws IOException {
        //Loading an existing PDF document
        String outputPath = outputFileName(inputFile,"png");
        PDDocument document = extractPDFFirstPage(inputFile);

        //Instantiating the PDFRenderer class
        PDFRenderer renderer = new PDFRenderer(document);

        //Rendering an image from the PDF document
        BufferedImage image = renderer.renderImage(0);

        //Writing the image to a file
        ImageIO.write(image, "JPEG", new File(outputPath));

        System.out.println("Image created");

        //Closing the document
        document.close();
        return outputPath;
    }

    private String toText(String inputFileName) throws IOException {
        String outputFileName = outputFileName(inputFileName,"txt");
        File inputFile =new File(inputFileName);
//        PDDocument pdf = extractPDFFirstPage(inputFileName);
        PDDocument pdf = PDDocument.load(new FileInputStream(inputFile));
        PDFTextStripper  stripper = new PDFTextStripper();
        stripper.setStartPage(0);
        stripper.setEndPage(1);
        String outputText = stripper.getText(pdf);
        writeTextToFile(outputText,outputFileName);
        return outputFileName;
    }

    private void toHtml(String inputFile) throws IOException{

    }

    private static String outputFileName(String input,String extension){
        int i = input.lastIndexOf('.');
        return input.substring(0,i+1) + extension;
    }

    private void writeTextToFile(String text, String path) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(path);
        byte[] strToBytes = text.getBytes();
        outputStream.write(strToBytes);
        outputStream.close();
    }

    private PDDocument extractPDFFirstPage1(String inputFile) throws IOException {
        //Loading an existing PDF document
        File file = new File(inputFile);
        PDDocument document = PDDocument.load(new FileInputStream(file));
        //Instantiating Splitter class
        Splitter splitter = new Splitter();
        splitter.setStartPage(1);
        splitter.setEndPage(2);
        //Saving each page as an individual document
        List<PDDocument> Pages = splitter.split(document);

        //splitting the pages of a PDF document
        Iterator<PDDocument> iterator = Pages.listIterator();

        //Creating an iterator
        int i = 1;
        if(iterator.hasNext()) {
            PDDocument document1 = iterator.next();
            document1.save("test.pdf");
            return document1;
        }
        return null;
    }


    private PDDocument extractPDFFirstPage(String inputFile) throws IOException {
        //Loading an existing PDF document
        File file = new File(inputFile);
        PDDocument document = PDDocument.load(new FileInputStream(file));
        //Instantiating Splitter class
        PageExtractor splitter = new PageExtractor(document);
        splitter.setStartPage(1);
        splitter.setEndPage(2);
        //Saving each page as an individual document
        PDDocument singlePage = splitter.extract();
        singlePage.save("test.pdf");
        return singlePage;
    }


    private String performOp(String op,String inputFile) throws IOException {
        switch (op){
            case "ToImage":
                return toImage(inputFile);
            case "ToText":
                return toText(inputFile);
            case "ToHTML":
                return toImage(inputFile);
            default:
                return null;
        }
    }

}
