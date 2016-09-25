package com.movebeam.kinesis;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;


/**
 * Hello world!
 *
 */
public class App 
{


    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        URL datafile = null;
        List<String> datafileUrls = Arrays.asList(
                "http://download-data.deutschebahn.com/static/datasets/callabike/20160615/HACKATHON_VEHICLE_CALL_A_BIKE.csv",
                "http://download-data.deutschebahn.com/static/datasets/callabike/20160615/HACKATHON_RENTAL_ZONE_CALL_A_BIKE.csv",
                "http://download-data.deutschebahn.com/static/datasets/callabike/20160615/HACKATHON_BOOKING_CALL_A_BIKE.zip",
                "http://download-data.deutschebahn.com/static/datasets/callabike/20160615/HACKATHON_CATEGORY_CALL_A_BIKE.csv",
                "http://download-data.deutschebahn.com/static/datasets/callabike/20160615/HACKATHON_EFFICIENCY_CALL_A_BIKE.zip",
                "http://download-data.deutschebahn.com/static/datasets/callabike/20160615/HACKATHON_AVAILABILITY_CALL_A_BIKE.zip"
                );
        try {

            for (String path:datafileUrls
                 ) {
                System.out.println( "Downloading " + path );

                String fileName = FilenameUtils.getName(path);
                String extension = FilenameUtils.getExtension(path);
                System.out.println("fileName : " + fileName);
                System.out.println("extension : " + extension);

                datafile = new URL(path);
                ReadableByteChannel rbc = Channels.newChannel(datafile.openStream());

                FileOutputStream fos = new FileOutputStream("/data/" + fileName);
                fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                System.out.println("Download finished");
                if (extension.equals("zip")) {
                    System.out.println("unpacking zip");
                    String source = "/data/" + fileName;
                    String destination = "/data";
                    ZipFile zipFile = new ZipFile(source);

                    zipFile.extractAll(destination);
                    System.out.println("deleting archive");
                    Files.delete (Paths.get(source));
                }
            }

            AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
            File dir = new File("/data");
            File[] directoryListing = dir.listFiles();
            if (directoryListing != null) {
                for (File child : directoryListing) {
                    try {
                        System.out.println("Uploading " + child.getName() +" to S3 from a file\n");
                        File file = new File(child.getAbsolutePath());
                        s3client.putObject(new PutObjectRequest(
                                "db-demo-nilsheuer", child.getName(), file));

                    } catch (AmazonServiceException ase) {
                        System.out.println("Caught an AmazonServiceException, which " +
                                "means your request made it " +
                                "to Amazon S3, but was rejected with an error response" +
                                " for some reason.");
                        System.out.println("Error Message:    " + ase.getMessage());
                        System.out.println("HTTP Status Code: " + ase.getStatusCode());
                        System.out.println("AWS Error Code:   " + ase.getErrorCode());
                        System.out.println("Error Type:       " + ase.getErrorType());
                        System.out.println("Request ID:       " + ase.getRequestId());
                    } catch (AmazonClientException ace) {
                        System.out.println("Caught an AmazonClientException, which " +
                                "means the client encountered " +
                                "an internal error while trying to " +
                                "communicate with S3, " +
                                "such as not being able to access the network.");
                        System.out.println("Error Message: " + ace.getMessage());
                    }
                }
            } else {
                // Handle the case where dir is not really a directory.
                // Checking dir.isDirectory() above would not be sufficient
                // to avoid race conditions with another process that deletes
                // directories.
            }



        }
        catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        catch (ZipException e) {
            e.printStackTrace();
        }


    }
}
