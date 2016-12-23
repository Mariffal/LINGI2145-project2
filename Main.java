import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map.Entry;

import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;


public class Main {

	public static void main(String[] args) {

		// Get the credentials
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/thomas/.aws/credentials), and is in valid format.",
                    e);
        }
        
        // Create new SQS object with region
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
        
        // Create new S3 object with region
        AmazonS3 s3 = new AmazonS3Client(credentials);
        s3.setRegion(usWest2);
        
        String queueUrl = "https://sqs.us-west-2.amazonaws.com/829489956151/bclassep2";
        String bucketName = "bclassep2";
        
        System.out.println("Receiving messages from MyQueue.\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        while(true) {
        	List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        	for (Message message : messages) {
        		
        		String body = message.getBody();
        		String bodySplitted[] = body.split(":");
        		
        		System.out.println(bodySplitted[1]);
        		S3Object object = s3.getObject(new GetObjectRequest(bucketName, bodySplitted[1]));
        		
        		try {
        			writeToFile(object.getObjectContent(), "/var/tmp/"+bodySplitted[1]);
        		} catch (IOException e) {
        			System.err.println(e.getMessage());
        			continue;
        		}
            
        		/* This block of code resize an image */
        		ConvertCmd cmd = new ConvertCmd();
        		IMOperation op = new IMOperation();
        		op.addImage("/var/tmp/"+bodySplitted[1]);
        		op.resize(800, 600, '>');
        		op.addImage("/var/tmp/resized-"+bodySplitted[1]);
        		try {
        			cmd.run(op);
        		} catch (Exception e) {
        			System.err.println(e.getMessage());
        		}
        		/* End of resizing */
        		
        		File newImage = new File("/var/tmp/resized-"+bodySplitted[1]);
        		File oldImage = new File("/var/tmp/"+bodySplitted[1]);
        		
        		System.out.println("Putting image as: events/" + bodySplitted[0]+"/"+bodySplitted[1]);
        		s3.putObject(new PutObjectRequest(bucketName, "events/"+bodySplitted[0]+"/"+bodySplitted[1], newImage));
        		
        		newImage.delete();
        		oldImage.delete();
        		
        		String s = message.getReceiptHandle();
        		sqs.deleteMessage(new DeleteMessageRequest(queueUrl, s));
        		
        		s3.deleteObject(bucketName, bodySplitted[1]);
        		
        		System.out.println("proccessed");
        	}
        }
		
	}
	
	private static void writeToFile(S3ObjectInputStream stream, String name) throws IOException {
		FileOutputStream fos = new FileOutputStream(name);
		while (true) {
			int str = stream.read();
			if(str == -1) break;
			fos.write(str);
		}
	}

}
