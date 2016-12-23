
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class LambdaFunctionHandler implements RequestHandler<S3Event, Object> {

	public static void main(String[] args) {
		// Lol
	}
	
	static final int BUFFER = 2048;
    static byte data[] = new byte[BUFFER];

	@Override
	public Object handleRequest(S3Event input, Context context) {
		context.getLogger().log("Test1");

		AWSCredentials credentials = new BasicAWSCredentials("AKIAICXILZ6LMT6WHF3Q", "vjULjKEMP/mp/psJbvAiSe+N6UkjswIICG5C6koP");

		for(S3EventNotificationRecord m : input.getRecords()) {

			String path = m.getS3().getObject().getKey(); 
			String bucketName = m.getS3().getBucket().getName();
			String[] split = path.split("/");    		
			String folder_events = split[0];
			String folder_eventhash = split[1];
			String image_name = split[2];

			AmazonS3 s3 = new AmazonS3Client(credentials);
			Region usWest2 = Region.getRegion(Regions.US_WEST_2);
			s3.setRegion(usWest2);

			ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
			.withBucketName(bucketName)
			.withPrefix(folder_events+"/"+folder_eventhash));

			File zipfile = new File("/tmp/"+folder_eventhash + ".zip");

			try {
				ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipfile)));
				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
					String key = objectSummary.getKey();
					
					try {
						s3.deleteObject(bucketName, folder_events+"/"+folder_eventhash+"/"+folder_eventhash+".zip");
					} catch (AmazonS3Exception e) {
						
					}
					
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

			AccessControlList acl = new AccessControlList();
			acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
			s3.putObject(new PutObjectRequest(bucketName, "events/zips/"+folder_eventhash+".zip", zipfile).withAccessControlList(acl));
			zipfile.delete();
		}

		return null;
	}

}
