package com.gd.kinesis;
/**
 * Sample Amazon Kinesis Application.
 * PutRecords will fail with a ProvisionedThroughputExceededException
 */

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class App {

    final static String payLoad = "{\"message\":{\"header\":{\"monitoring\":{\"initialize-at-collector\":1455102165967,\"before-submit-to-composer\":1455102165967,\"start-composer-query\":1455102165968,\"end-composer-query\":1455102165984},\"context-ref\":\"BET.BET-PLACEMENT\",\"activity-id\":3,\"activity-descriptor\":{\"name\":\"BET\",\"id\":1,\"description\":\"Bet Activity Class\"},\"queue-id\":1893,\"sequencing-key\":{\"sequencing-descriptor\":{\"name\":\"BET\",\"id\":1,\"description\":\"Bet Sequence Class\"},\"sequencing-id\":11},\"crud-type\":\"CREATE\",\"unique-id\":\"ddaac1b5-ca25-4099-bd48-3bc43ebc02d2\",\"time-stamp\":1455102165000},\"state\":{\"bets\":[{\"receipt\":\"O\\/0000011 \\/ 0000002\",\"is-cashed-out\":false,\"is-settled\":false,\"is-confirmed\":true,\"stake\":{\"amount\":\"2.00\",\"currency-ref\":\"EUR\",\"stake-per-line\":\"2.00\",\"free-bet\":\"0.00\"},\"leg\":[{\"result\":\"-\",\"leg-sort\":\"--\",\"index\":0,\"win-place-ref\":\"WIN\",\"leg-parts\":[{\"outcome-ref\":582,\"market-ref\":222,\"event-ref\":27,\"places\":1,\"is-in-running\":\"false\"}],\"price\":{\"num\":50,\"den\":100,\"price-type-ref\":\"LP\",\"decimal\":\"1.5\"}},{\"result\":\"-\",\"leg-sort\":\"--\",\"index\":0,\"win-place-ref\":\"WIN\",\"leg-parts\":[{\"outcome-ref\":540,\"market-ref\":214,\"event-ref\":26,\"places\":1,\"is-in-running\":\"true\"}],\"price\":{\"num\":50,\"den\":100,\"price-type-ref\":\"LP\",\"decimal\":\"1.5\"}},{\"result\":\"-\",\"leg-sort\":\"--\",\"index\":0,\"win-place-ref\":\"WIN\",\"leg-parts\":[{\"outcome-ref\":498,\"market-ref\":206,\"event-ref\":25,\"places\":1,\"is-in-running\":\"false\"}],\"price\":{\"num\":50,\"den\":100,\"price-type-ref\":\"LP\",\"decimal\":\"1.5\"}}],\"lines\":{\"win\":0,\"lose\":0,\"voided\":0,\"number\":1},\"customer-ref\":11,\"source\":{\"channel-ref\":\"I\"},\"account-ref\":11,\"id\":3,\"creation-date\":\"2016-02-10T12:02:45.000+0100\",\"payout\":{\"winnings\":\"0.00\",\"refunds\":\"0.00\",\"potential\":\"6.75\"},\"is-pool-bet\":false,\"bet-type-ref\":\"TBL\",\"is-cancelled\":false,\"is-parked\":false,\"placed-at\":\"2016-02-10T12:02:45.000+0100\"}]},\"supporting-state\":{\"outcome\":[{\"event-name\":\"|CAGLIARI| |vs| |PALERMO|\",\"outcome-name\":\"|CAGLIARI|\",\"event-start-time\":\"2016-02-18T19:00:00.000+0100\",\"outcome-ref\":540,\"event-ref\":26,\"category-ref\":\"FOOTBALL\",\"market-ref\":214,\"class-ref\":\"1\",\"market-name\":\"|Match Result|\",\"class-sort\":\"FB\"},{\"event-name\":\"|TORINO| |vs| |CHIEVO|\",\"outcome-name\":\"|TORINO|\",\"event-start-time\":\"2016-02-18T19:00:00.000+0100\",\"outcome-ref\":582,\"event-ref\":27,\"category-ref\":\"FOOTBALL\",\"market-ref\":222,\"class-ref\":\"1\",\"market-name\":\"|Match Result|\",\"class-sort\":\"FB\"},{\"event-name\":\"|VERONA| |vs| |EMPOLI |\",\"outcome-name\":\"|VERONA|\",\"event-start-time\":\"2016-02-18T19:00:00.000+0100\",\"outcome-ref\":498,\"event-ref\":25,\"category-ref\":\"FOOTBALL\",\"market-ref\":206,\"class-ref\":\"1\",\"market-name\":\"|Match Result|\",\"class-sort\":\"FB\"}]}},\"metadata\":{\"source\":{\"name\":\"activity-feed\",\"topic\":\"bets\",\"environment\":\"test\",\"received-at\":\"2016-02-03T14:33:05.000+0100\"}}}";

    public static void main(String[] args) throws Exception {

        String accessKey = "";
        String secretKey = "";
        String streamName = "";
        String region = "";

        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        final AmazonKinesis kinesisClient = AmazonKinesisClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(region)
                .build();

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();

        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<PutRecordsRequestEntry>();
        for (int i = 0; i < 500; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(ByteBuffer.wrap(payLoad.getBytes()));
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult = null;
        try {
            putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
            System.out.println("Put Result" + putRecordsResult);
        } catch (Exception e) {
            System.out.println("Exception:" + e.getMessage());
        }

    }
}
