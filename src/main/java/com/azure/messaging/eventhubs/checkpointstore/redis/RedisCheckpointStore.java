// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.messaging.eventhubs.checkpointstore.redis;

import com.azure.core.util.CoreUtils;
import com.azure.core.util.logging.ClientLogger;
import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

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

    // "{fully qualified namespace}/{eventhub name}/{consumer group}/ownerships/{partition id}"
    private static final String OWNERSHIP_KEY = "%s/%s/%s/ownerships/%s";
    // "{fully qualified namespace}/{eventhub name}/{consumer group}/checkpoints"
    private static final String CHECKPOINTS_KEY = "%s/%s/%s/checkpoints";
    // "{fully qualified namespace}/{eventhub name}/{consumer group}/checkpoints/{partition id}"
    private static final String PARTITION_CHECKPOINT_KEY = CHECKPOINTS_KEY + "/%s";

    private final RedisClient redisClient;
    private final RedisReactiveCommands<String, String> commands;
    private final RedisCommands<String, String> syncCommands;
    private final StatefulRedisConnection<String, String> connection;

    public RedisCheckpointStore(RedisClient redisClient) {
        this.redisClient = redisClient;
        this.connection = redisClient.connect();
        this.commands = connection.reactive();
        this.syncCommands = connection.sync();
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
    public Flux<PartitionOwnership> listOwnership(String fullyQualifiedNamespace, String eventHubName,
                                                  String consumerGroup) {
        String ownershipKey = String.format(OWNERSHIP_KEY, fullyQualifiedNamespace,
            eventHubName, consumerGroup, "*");

        return commands.keys(ownershipKey)
                       .flatMap(partitionId ->
                           commands.get(partitionId)
                                   .map(ownerIdAndEtag -> convertToPartitionOwnership(fullyQualifiedNamespace,
                                       eventHubName, consumerGroup, partitionId, ownerIdAndEtag)));
    }

    private PartitionOwnership convertToPartitionOwnership(String fullyQualifiedNamespace, String eventHubName,
                                                     String consumerGroup, String partitionId, String ownerIdAndEtag) {
        String[] metadata = ownerIdAndEtag.split("_");
        String etag = metadata[1];
        PartitionOwnership partitionOwnership =
            new PartitionOwnership();

        partitionOwnership.setOwnerId(metadata[0]);
        partitionOwnership.setETag(etag);
        partitionOwnership.setPartitionId(partitionId);
        partitionOwnership.setConsumerGroup(consumerGroup);
        partitionOwnership.setEventHubName(eventHubName);
        partitionOwnership.setFullyQualifiedNamespace(fullyQualifiedNamespace);
        partitionOwnership.setLastModifiedTime(Long.parseLong(etag));

        return partitionOwnership;
    }

    @Override
    public Flux<PartitionOwnership> claimOwnership(List<PartitionOwnership> requestedPartitionOwnerships) {

        return Flux.fromIterable(requestedPartitionOwnerships).flatMap(partitionOwnership -> {
            String partitionId = partitionOwnership.getPartitionId();
            try {
                String fullyQualifiedNamespace = partitionOwnership.getFullyQualifiedNamespace();
                String eventHubName = partitionOwnership.getEventHubName();
                String consumerGroup = partitionOwnership.getConsumerGroup();
                String ownershipKey = String.format(OWNERSHIP_KEY, fullyQualifiedNamespace,
                    eventHubName, consumerGroup, partitionId);

                if (CoreUtils.isNullOrEmpty(partitionOwnership.getETag())) {
                    String eTag = String.valueOf(System.currentTimeMillis());
                    return commands
                        .setnx(ownershipKey, partitionOwnership.getOwnerId() + "_" + eTag)
                        .flatMapMany(
                            result -> {
                                if (Boolean.TRUE.equals(result)) {
                                    partitionOwnership.setETag(eTag);
                                    return Mono.just(partitionOwnership);
                                } else {
                                    throw new IllegalStateException("Fail to claim the partition");
                                }
                            },
                            error -> {
                                logger.atVerbose()
                                      .addKeyValue(PARTITION_ID_LOG_KEY, partitionId)
                                      .log(Messages.CLAIM_ERROR, error);
                                return Mono.empty();
                            },
                            Mono::empty
                        );
                } else {
                    return commands
                        .watch(ownershipKey)
                        .doFinally(s -> commands.unwatch().block())
                        .flatMapMany(
                            s -> {
                                updateOwnerId(ownershipKey, partitionOwnership);
                                return Mono.just(partitionOwnership);
                            },
                            error -> {
                                logger.atVerbose()
                                      .addKeyValue(PARTITION_ID_LOG_KEY, partitionId)
                                      .log(Messages.CLAIM_ERROR, error);
                                return Mono.empty();

                            },
                            Mono::empty
                        );
                }
            } catch (Exception ex) {
                logger.atWarning()
                      .addKeyValue(PARTITION_ID_LOG_KEY, partitionOwnership.getPartitionId())
                      .log(Messages.CLAIM_ERROR, ex);
                return Mono.empty();
            }
        });
    }

    private void updateOwnerId(String ownershipKey, PartitionOwnership partitionOwnership) {
        String ownerIdAndEtag = syncCommands.get(ownershipKey);
        if (ownerIdAndEtag.endsWith(partitionOwnership.getETag())) {
            syncCommands.multi();
            String eTag = String.valueOf(System.currentTimeMillis());
            syncCommands.set(ownershipKey, partitionOwnership.getOwnerId() + "_" + eTag);
            TransactionResult result = syncCommands.exec();
            if (!result.wasDiscarded()) {
                partitionOwnership.setETag(eTag);
                logger.atInfo()
                    .addKeyValue(PARTITION_ID_LOG_KEY, partitionOwnership.getPartitionId())
                    .log("Claimed successfully!");
            } else {
                throw new IllegalStateException("Fail to claim the partition");
            }
        }
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

                           String[] metadata = checkpointMetadata.split(":");
                           String seqNumberValue = metadata[0];
                           String offsetValue = metadata[1];
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
