// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.messaging.eventhubs.checkpointstore.redis;

import com.azure.core.util.logging.ClientLogger;
import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisCheckpointStore implements CheckpointStore, AutoCloseable {
    private final ClientLogger logger = new ClientLogger(RedisCheckpointStore.class);
    private static final String SEQUENCE_NUMBER = "sequencenumber";
    private static final String OFFSET = "offset";
    private static final String OWNER_ID = "ownerid";
    private static final String ETAG = "eTag";

    private static final String BLOB_PATH_SEPARATOR = "/";
    private static final String CHECKPOINT_PATH = "/checkpoint/";
    private static final String OWNERSHIP_PATH = "/ownership/";

    // logging keys, consistent across all AMQP libraries and human-readable
    private static final String PARTITION_ID_LOG_KEY = "partitionId";
    private static final String OWNER_ID_LOG_KEY = "ownerId";
    private static final String SEQUENCE_NUMBER_LOG_KEY = "sequenceNumber";
    private static final String BLOB_NAME_LOG_KEY = "blobName";
    private static final String OFFSET_LOG_KEY = "offset";
    private final RedisClient redisClient;
    private final RedisReactiveCommands<String, String> commands;
    private final StatefulRedisConnection<String, String> connection;

    public RedisCheckpointStore(RedisClient redisClient) {
        this.redisClient = redisClient;
        this.connection = redisClient.connect();
        this.commands = connection.reactive();
    }

    @Override
    public void close() throws Exception {
        logger.verbose("Shutting down redis client");
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Override
    public Flux<PartitionOwnership> listOwnership(String s, String s1, String s2) {
        return null;
    }

    @Override
    public Flux<PartitionOwnership> claimOwnership(List<PartitionOwnership> requestedPartitionOwnerships) {

        //        return Flux.fromIterable(requestedPartitionOwnerships).flatMap(partitionOwnership -> {
        //            try {
        //                String partitionId = partitionOwnership.getPartitionId();
        //                String blobName = getBlobName(partitionOwnership.getFullyQualifiedNamespace(),
        //                    partitionOwnership.getEventHubName(), partitionOwnership.getConsumerGroup(), partitionId,
        //                    OWNERSHIP_PATH);
        //
        //                if (!blobClients.containsKey(blobName)) {
        //                    blobClients.put(blobName, blobContainerAsyncClient.getBlobAsyncClient(blobName));
        //                }
        //            }
        //        });


        return null;
    }

    @Override
    public Flux<Checkpoint> listCheckpoints(String fullyQualifiedNamespace, String eventHubName,
                                            String consumerGroup) {
        String checkpointSetKey = String.format(CHECKPOINTS_KEY, fullyQualifiedNamespace,
            eventHubName, consumerGroup);
        return commands.hgetall(checkpointSetKey)
                       .map(kv -> {
                           String partitionId = kv.getKey();
                           String checkpointMetadata = kv.getValue();
                           Checkpoint checkpoint = new Checkpoint();

                           String[] metadatas = checkpointMetadata.split(":");
                           String seqNumberValue = metadatas[0];
                           String offsetValue = metadatas[1];
                           Long sequenceNumber = seqNumberValue != null ? Long.valueOf(seqNumberValue) : null;
                           Long offset = offsetValue != null ? Long.valueOf(offsetValue) : null;
                           checkpoint.setFullyQualifiedNamespace(fullyQualifiedNamespace);
                           checkpoint.setEventHubName(eventHubName);
                           checkpoint.setConsumerGroup(consumerGroup);
                           checkpoint.setPartitionId(partitionId);
                           checkpoint.setSequenceNumber(sequenceNumber);
                           checkpoint.setOffset(offset);
                           return checkpoint;
                       });
    }

    // "{fully qualified namespace}/{eventhub name}/{consumer group}/checkpoints"
    private static final String CHECKPOINTS_KEY = "%s/%s/%s/checkpoints";
    // "{fully qualified namespace}/{eventhub name}/{consumer group}/checkpoints/{partition id}"
    private static final String PARTITION_CHECKPOINT_KEY = CHECKPOINTS_KEY + "/%s";

    /**
     * Using a redis hash to store all checkpoints within one specific consumer group
     * <p>
     * key: {fully qualified namespace}/{eventhub name}/{consumer group}/checkpoints value: {partition id} :
     * "{sequenceNumber}:{offset}"
     */

    @Override
    public Mono<Void> updateCheckpoint(Checkpoint checkpoint) {
        if (checkpoint == null || (checkpoint.getSequenceNumber() == null && checkpoint.getOffset() == null)) {
            throw logger.logExceptionAsWarning(Exceptions
                .propagate(new IllegalStateException(
                    "Both sequence number and offset cannot be null when updating a checkpoint")));
        }
        String partitionId = checkpoint.getPartitionId();
        String checkpointSetKey = String.format(CHECKPOINTS_KEY, checkpoint.getFullyQualifiedNamespace(),
            checkpoint.getEventHubName(),
            checkpoint.getConsumerGroup());

        String sequenceNumber = checkpoint.getSequenceNumber() == null ? null :
            String.valueOf(checkpoint.getSequenceNumber());
        String offset = checkpoint.getOffset() == null ? null : String.valueOf(checkpoint.getOffset());
        String checkpointMetadata = sequenceNumber + ":" + offset;

        return commands.hset(checkpointSetKey, partitionId, checkpointMetadata).then();
    }
}
