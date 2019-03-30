import com.google.common.base.Preconditions;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.Random;

public class StickyRandomPartitioner implements Partitioner {
  Long stickTime;
  TemporalUnit timeUnit;
  Clock clock;

  /**
   * Constructor to be used with a call to configure()
   */
  public  StickyRandomPartitioner() {
    this.clock = Clock.systemUTC();
  }

  /**
   * Constructor for testing
   * @param stickTime
   * @param timeUnit
   * @param clock
   */
  public StickyRandomPartitioner(Long stickTime, TemporalUnit timeUnit, Clock clock) {
    setStickTime(stickTime);
    setTimeUnit(timeUnit);
    this.clock = clock;
  }

  public void setStickTime(Long stickTime) {
    this.stickTime = stickTime;
  }

  public void setTimeUnit(TemporalUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  /**
   * TODO Performance could be improved by updating the current time in a thread instead of on demand
   * @param topic
   * @param key
   * @param cluster
   * @return
   */
  public int partition(String topic, Long key, Cluster cluster) {
    LocalDateTime now = LocalDateTime.now(clock);
    long seconds = now.toEpochSecond(ZoneOffset.of(clock.getZone().getId()));
    long stickDuration = timeUnit.getDuration().getSeconds() * stickTime;
    long partitionKey = Math.floorDiv(seconds, stickDuration) + key;

    return new Random(partitionKey).nextInt(cluster.availablePartitionsForTopic(topic).size());
  }

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    Preconditions.checkArgument(key instanceof Long);
    return partition(topic, (Long) key, cluster);
  }

  @Override
  public void close() {
    //No-op
  }

  @Override
  /**
   * TODO implement
   */
  public void configure(Map<String, ?> configs) {

  }
}
