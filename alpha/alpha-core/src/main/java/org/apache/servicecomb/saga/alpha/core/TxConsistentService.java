/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.servicecomb.saga.alpha.core;

import static org.apache.servicecomb.saga.common.EventType.SagaEndedEvent;
import static org.apache.servicecomb.saga.common.EventType.TxAbortedEvent;
import static org.apache.servicecomb.saga.common.EventType.TxStartedEvent;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.Context;
import org.apache.servicecomb.saga.common.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;


public class TxConsistentService {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final TxEventRepository eventRepository;
  private final Map<String, Map<String, OmegaCallback>> omegaCallbacks;

  //  private final List<String> types = Arrays.asList(TxStartedEvent.name(), SagaEndedEvent.name(), TxAbortedEvent.name());
  private final List<String> types = Arrays.asList(TxStartedEvent.name(), SagaEndedEvent.name());

  private final JedisPool jedisPool;

  public TxConsistentService(TxEventRepository eventRepository, Map<String, Map<String, OmegaCallback>> omegaCallbacks) {
    this.eventRepository = eventRepository;
    this.omegaCallbacks = omegaCallbacks;

    JedisPoolConfig config = new JedisPoolConfig();
    config.setMaxIdle(50);
    config.setMinIdle(1);
    config.setMaxTotal(100);
    config.setMaxWaitMillis(30000);
    this.jedisPool = new JedisPool(config, "redis.servicecomb", 6379, 30000);

    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    service.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
//        System.out.println("scanning redis in background " + new Date());
        scanInReids();
      }
    }, 3000, 5000, TimeUnit.MILLISECONDS);
  }

  private void scanInReids() {
    System.out.println("scanning redis in background " + new Date());
    String pattern = "*" + EventType.TxAbortedEvent.name() + "*";

    Jedis jedis = null;

    try {
      jedis = jedisPool.getResource();
      Set<String> keys = jedis.keys(pattern);

      for (String key : keys) {
        String[] parts = key.split(":");
        String gid = parts[0];

        String globalTxKey = key.replaceAll(EventType.TxAbortedEvent.name(), EventType.SagaStartedEvent.name());
        String globalTxStatus = jedis.hget(globalTxKey, "status");
        if (globalTxStatus != null && globalTxStatus.equals("scanning")) {
          continue;
        }

        // Find the coresponding TxStartedEvent
        String startedTxKey = key.replaceAll(EventType.TxAbortedEvent.name(), EventType.TxStartedEvent.name());
        Map<String, String> map = jedis.hgetAll(startedTxKey);

        if (map.isEmpty() == false) {
          TxEvent eventToCompensate = mapToTxEvent(map);
          Map<String, OmegaCallback> callbacks = omegaCallbacks.get(eventToCompensate.serviceName());
          if (callbacks.isEmpty() == false) {
            System.out.println("assume that we send compensation command for event " + eventToCompensate.instanceId());
            finishTx(eventToCompensate);
            // Run compensate in the background
//          Context.current().fork().run(new Runnable() {
//            @Override
//            public void run() {
//              System.out.println("Sending compenstation command to " + eventToCompensate.instanceId());
//              try {
//                OmegaCallback callback = callbacks.get(eventToCompensate.instanceId());
//                callback.compensate(eventToCompensate);
//                // Remove the tx in redis
//                finishTx(eventToCompensate);
//              } catch (Exception e) {
//                System.out.println("e: " + e.getMessage());
//              }
//            }
//          });
          }
        }
      }

    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      if (jedis != null) {

        jedis.close();
      }
    }

  }


  public boolean handle(TxEvent event) {
    if (types.contains(event.type()) && isGlobalTxAborted(event)) {
      LOG.info("Transaction event {} rejected, because its parent with globalTxId {} was already aborted",
              event.type(), event.globalTxId());
      return false;
    }

//    eventRepository.save(event);
    save(event);


    return true;
  }

  private String ensureString(String str) {
    return str == null ? "" : str;
  }

  private void save(TxEvent event) {
    String type = event.type();
    String globalTxId = event.globalTxId();
    String localTxId = event.localTxId();

    String key = globalTxId + ":" + type + ":" + localTxId;

    HashMap<String, String> map = new HashMap<String, String>();
    map.put("globalTxId", globalTxId);
    map.put("localTxId", localTxId);
    map.put("type", type);
    map.put("instanceId", event.instanceId());
    map.put("parentTxId", ensureString(event.parentTxId()));
    map.put("serviceName", ensureString(event.serviceName()));
    map.put("compensationMethod", event.compensationMethod());
    map.put("creationTime", String.valueOf(event.creationTime().getTime()));
    map.put("payloads", new String(event.payloads()));

    Jedis jedis = null;
    try {
      jedis = jedisPool.getResource();

      if (type.equals(EventType.SagaStartedEvent.name())) {
        map.put("status", "started");
      }


      String ok = jedis.hmset(key, map);

      if (type.equals(EventType.SagaEndedEvent.name())) {
        // Remove the related keys
        finishTx(event);
      }

      if (type.equals(EventType.TxAbortedEvent.name())) {
        String globalTxKey = globalTxId + ":" + EventType.SagaStartedEvent + ":" + globalTxId;

        String globalTxStatus = jedis.hget(globalTxKey, "status");
        if (globalTxStatus.equals("scanning")) {
          return;
        }

        // Mark the transaction status as 'scanning'
        jedis.hset(globalTxKey, "status", "scanning");

      }

    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  private void finishTx(TxEvent event) {
    Jedis jedis = jedisPool.getResource();

    try {

      String globalTxId = event.globalTxId();
      Set<String> keys = jedis.keys(globalTxId + "*");

      for (String key : keys) {
        jedis.del(key);
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }

  }

  private boolean isGlobalTxAborted(TxEvent event) {
//    String type = event.type();
    String globalTxId = event.globalTxId();

    String key = globalTxId + ":" + EventType.SagaStartedEvent + ":" + globalTxId;

    Jedis jedis = jedisPool.getResource();
    boolean isAborted = false;
    try {
      String status = jedis.hget(key, "status");
      isAborted = (status.equals("started") == false);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
    return isAborted;
//    boolean isAborted = !eventRepository.findTransactions(event.globalTxId(), TxAbortedEvent.name()).isEmpty();
//
//    return isAborted;
  }

  private void scan(String globalTxId) {
    System.out.println("scanning " + globalTxId + ", this should be done in the background");
    // Scan the keys
    String keyPattern = globalTxId + ":Tx[Started|Ended]*";
    ScanParams params = new ScanParams();
    params.count(20);
    params.match(keyPattern);

    ScanResult<String> result;
    String cursor = "0";
    List<String> keysToHandle = new ArrayList<String>();

    Jedis jedis = jedisPool.getResource();
    while (true) {
      result = jedis.scan(cursor, params);
      List<String> keys = result.getResult();

      keysToHandle.addAll(keys);
      cursor = result.getStringCursor();

      if (cursor.equals("0")) {
        break;
      }
    }

    while (keysToHandle.isEmpty() == false) {
      String k = keysToHandle.remove(0);
      String[] parts = k.split(":");
      String gid = parts[0];
      String txtype = parts[1];
      String lid = parts[2];

      String pairType = txtype.equals("TxStartedEvent") ? "TxEndedEvent" : "TxStartedEvent";
      String pairKey = gid + ":" + pairType + ":" + lid;
      if (keysToHandle.contains(pairKey)) {
        // TODO Do the real compensation job
//        System.out.println("Compensate " + gid + ":" + lid);
        Map<String, String> map = jedis.hgetAll(gid + ":" + EventType.TxStartedEvent + ":" + lid);
        if (map.isEmpty() == false) {
          TxEvent eventToCompensate = mapToTxEvent(map);
          Map<String, OmegaCallback> callbacks = omegaCallbacks.get(eventToCompensate.serviceName());
          if (callbacks.isEmpty() == false) {
            // Run compensate in the background
            Context.current().fork().run(new Runnable() {
              @Override
              public void run() {
                System.out.println("Sending compenstation command to " + eventToCompensate.instanceId());
                try {
                  OmegaCallback callback = callbacks.get(eventToCompensate.instanceId());
                  callback.compensate(eventToCompensate);
                } catch (Exception e) {
                  System.out.println("e: " + e.getMessage());
                }
              }
            });
          }
        }

        keysToHandle.remove(pairKey);
      } else {
//        System.out.println("Redundent tx: " + gid + ":" + lid);
      }

      keysToHandle.remove(k);
    }


  }

  private void compensate(TxEvent event) {

  }

  private static TxEvent mapToTxEvent(Map<String, String> map) {
        /*
          String serviceName,
          String instanceId,
          String globalTxId,
          String localTxId,
          String parentTxId,
          String type,
          String compensationMethod,
          byte[] payloads
         */
    TxEvent event = new TxEvent(
            map.get("serviceName"),
            map.get("instanceId"),
            map.get("globalTxId"),
            map.get("localTxId"),
            map.get("parentTxId"),
            map.get("type"),
            map.get("compensationMethod"),
            map.get("payloads").getBytes()
    );

    return event;
  }
}
