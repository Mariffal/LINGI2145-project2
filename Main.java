import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class Main {
	
	static final int BUFFER = 2048;
    static byte data[] = new byte[BUFFER];

	public static void main(String[] args) {
		
		String bucketName = "oknowitworks";
		String folder_events = "events";
		String folder_eventhash = "753a03de62db0227df0a9969225299c2";
		
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
		
		AmazonS3 s3 = new AmazonS3Client(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        s3.setRegion(usWest2);
        
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
        .withBucketName(bucketName)
        .withPrefix(folder_events+"/"+folder_eventhash));
        
        File zipfile = new File(folder_eventhash + ".zip");
        
        try {
			ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipfile)));
	        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        		String key = objectSummary.getKey();
        		S3Object object = s3.getObject(new GetObjectRequest(bucketName, key));
        		S3ObjectInputStream is = object.getObjectContent();
        		
        		BufferedInputStream bis = new BufferedInputStream(is, BUFFER);
        		
        		String name = key.split("/")[2];
        		ZipEntry zep = new ZipEntry(name);
        		zos.putNextEntry(zep);
        		
        		int count;
        		while((count = bis.read(data, 0, BUFFER)) != -1) {
        			zos.write(data, 0, count);
        		}
        		zos.closeEntry();
        		
        		bis.close();	
	        }
	        zos.close();
			
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
        
        s3.putObject(bucketName, folder_events+"/"+folder_eventhash+"/"+folder_eventhash+".zip", zipfile);
        zipfile.delete();
	}

}
