package build.buildfarm.instance.shard;

import java.util.UUID;
import redis.clients.jedis.UnifiedJedis;

class CorrelatedInvocations {
  private final String prefix;
  private final long maxEntryTimeout;

  CorrelatedInvocations(String prefix, int maxEntryTimeout) {
    this.prefix = prefix;
    this.maxEntryTimeout = maxEntryTimeout;
  }

  public void add(UnifiedJedis jedis, UUID correlatedInvocationsId, UUID toolInvocationId) {
    String indexKey = createKey(correlatedInvocationsId);
    jedis.sadd(indexKey, toolInvocationId.toString());
    jedis.expire(indexKey, maxEntryTimeout);
  }

  private String createKey(UUID correlatedInvocationsId) {
    return prefix + ":" + correlatedInvocationsId;
  }
}
