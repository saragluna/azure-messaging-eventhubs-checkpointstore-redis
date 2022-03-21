package com.azure.messaging.eventhubs.checkpointstore.redis;

import com.azure.messaging.eventhubs.models.Checkpoint;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.Assert.*;

public class RedisCheckpointStoreTest {
    private static final String REDIS_IMAGE = "redis:6.2.6-alpine";
    private static final int REDIS_PORT = 6379;

    //    @Rule
    //    public GenericContainer redis = new GenericContainer(DockerImageName.parse(REDIS_IMAGE))
    //        .withExposedPorts(REDIS_PORT);
    RedisClient redisClient;
    RedisCheckpointStore redisCheckpointStore;

    @Before
    public void setUp() throws Exception {
        //        String address = redis.getHost();
        //        Integer port = redis.getFirstMappedPort();

        //        String
        //        RedisURI redisURI = RedisURI.builder().withHost(address).withPort(port).build();
        RedisURI redisURI = RedisURI.builder().withHost("localhost").build();
        redisClient = RedisClient.create(redisURI);
        redisCheckpointStore = new RedisCheckpointStore(redisClient);

    }

    @After
    public void tearDown() throws Exception {
        redisCheckpointStore.close();
    }

    @Test
    public void listOwnership() {
    }

    @Test
    public void claimOwnership() {
    }

    @Test
    public void listCheckpoints() {
    }

    @Test
    public void updateCheckpoint() throws Exception {

        Checkpoint checkpoint = new Checkpoint();
        String fullyQualifiedNamespace = "namespace1";
        checkpoint.setFullyQualifiedNamespace(fullyQualifiedNamespace);
        String eventHubName = "eh1";
        checkpoint.setEventHubName(eventHubName);
        String consumerGroup = "cg1";
        checkpoint.setConsumerGroup(consumerGroup);
        checkpoint.setPartitionId("p1");
        checkpoint.setOffset(1L);
        checkpoint.setSequenceNumber(2L);

        Mono<Void> updateResult = redisCheckpointStore.updateCheckpoint(checkpoint);
        StepVerifier.create(updateResult)
                    .expectComplete()
                    .verify()
        ;


        Flux<Checkpoint> listCheckpoints = redisCheckpointStore.listCheckpoints(fullyQualifiedNamespace, eventHubName
            , consumerGroup);

        StepVerifier.create(listCheckpoints)
                    .expectNextMatches(new Predicate<Checkpoint>() {
                        @Override
                        public boolean test(Checkpoint c) {
                            return RedisCheckpointStoreTest.equals(checkpoint, c);
                        }
                    })
                    .expectComplete()
                    .verify();
    }

    private static boolean equals(Checkpoint c1, Checkpoint c2) {
        if (c1 == null && c2 == null) {
            return true;
        }
        if (c1 != null && c2 != null) {
            return Objects.equals(c1.getFullyQualifiedNamespace(), c2.getFullyQualifiedNamespace()) &&
                Objects.equals(c1.getEventHubName(), c2.getEventHubName()) &&
                Objects.equals(c1.getConsumerGroup(), c2.getConsumerGroup()) &&
                Objects.equals(c1.getPartitionId(), c2.getPartitionId()) &&
                Objects.equals(c1.getOffset(), c2.getOffset()) &&
                Objects.equals(c1.getSequenceNumber(), c2.getSequenceNumber());
        } else {
            return false;
        }
    }
}