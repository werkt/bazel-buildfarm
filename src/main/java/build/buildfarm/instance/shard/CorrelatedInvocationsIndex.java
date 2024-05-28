package build.buildfarm.instance.shard;

import java.util.UUID;
import redis.clients.jedis.UnifiedJedis;

class CorrelatedInvocationsIndex {
  private final String prefix;
  private final long maxEntryTimeout;

  CorrelatedInvocationsIndex(String prefix, int maxEntryTimeout) {
    this.prefix = prefix;
    this.maxEntryTimeout = maxEntryTimeout;
  }

  public void add(UnifiedJedis jedis, String scope, String key, UUID correlatedInvocationsId) {
    String indexKey = createKey(scope, key);
    jedis.sadd(indexKey, correlatedInvocationsId.toString());
    jedis.expire(indexKey, maxEntryTimeout);
  }

  private String createKey(String scope, String key) {
    // FIXME might need to url escape scope/keys
    return prefix + ":" + scope + "=" + key;
  }
}
