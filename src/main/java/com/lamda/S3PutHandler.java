package com.lamda;

import java.net.URLDecoder;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Creates Replica of the bucket
 * 
 * @author Anant Goswami 29-12-2017
 */
public class S3PutHandler implements RequestHandler<S3Event, Context> {

	static final AmazonS3 S3_CLIENT = AmazonS3ClientBuilder.defaultClient();

	public Context handleRequest(S3Event event, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("Start of handleRequest ");

		S3EventNotificationRecord s3event = event.getRecords().get(0);

		logger.log("Bucket object uploaded on : " + s3event.getS3().getBucket().getName());
		logger.log("Uploaded Object Key : " + s3event.getS3().getObject().getKey());

		try {
			String bucketName = s3event.getS3().getBucket().getName();
			String replicaBucketName = bucketName + "-replica";
			String objectKey = s3event.getS3().getObject().getKey().replace("+", " ");
			objectKey = URLDecoder.decode(objectKey, "UTF-8");

			createBucket(replicaBucketName, s3event.getAwsRegion(), logger);

			S3_CLIENT.copyObject(bucketName, objectKey, replicaBucketName, objectKey);

			listObjects(bucketName, logger);
			listObjects(replicaBucketName, logger);
		} catch (Exception e) {
			e.printStackTrace();
		}

		logger.log("Function :  " + context.getFunctionName());
		logger.log("Remaining Time in Millis : " + context.getRemainingTimeInMillis());
		logger.log("Memory Limit : " + context.getMemoryLimitInMB() + " MB");

		return context;
	}

	void listObjects(String bucketName, LambdaLogger logger) {

		logger.log("Bucket : " + bucketName + " Objects Listing - ");

		ObjectListing list = S3_CLIENT.listObjects(bucketName);
		List<S3ObjectSummary> objSummaryList = list.getObjectSummaries();

		objSummaryList.forEach(object -> {
			logger.log("Bucket : " + object.getBucketName() + " | key : " + object.getKey() + " | Last Modified : "
					+ object.getLastModified() + " | Size : " + object.getSize());
		});
	}

	void createBucket(String bucketName, String region, LambdaLogger logger) {

		if (!S3_CLIENT.doesBucketExist(bucketName)) {
			CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketName, region);
			Bucket bucket = S3_CLIENT.createBucket(createBucketRequest);

			logger.log("Created Bucket Name" + bucket.getName() + " | Creation :" + bucket.getCreationDate());
		}

	}

}
