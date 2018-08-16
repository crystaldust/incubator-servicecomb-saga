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

import org.apache.servicecomb.saga.common.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class TxConsistentService {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final TxEventRepository eventRepository;
  private final Map<String, Map<String, OmegaCallback>> omegaCallbacks;

//  private final List<String> types = Arrays.asList(TxStartedEvent.name(), SagaEndedEvent.name(), TxAbortedEvent.name());
  private final List<String> types = Arrays.asList(TxStartedEvent.name(), SagaEndedEvent.name());

  public TxConsistentService(TxEventRepository eventRepository, Map<String, Map<String, OmegaCallback>> omegaCallbacks) {
    this.eventRepository = eventRepository;
    this.omegaCallbacks = omegaCallbacks;
  }

  private static Jedis jedis = new Jedis("redis.servicecomb");

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


    if (type.equals(EventType.SagaStartedEvent.name())) {
      map.put("status", "started");
    }
    try {
      String ok = jedis.hmset(key, map);
//      System.out.print(ok);
    } catch (Exception e) {
      System.out.println(e);
    }

    if (type.equals(EventType.TxAbortedEvent.name())) {
      // Try to scan & compensate
      try {
        String globalTxKey = globalTxId + ":" + EventType.SagaStartedEvent + ":" + globalTxId;
        Long ok = jedis.hset(globalTxKey, "status", "scanning");
//        System.out.println(ok);
      } catch (Exception e) {
        System.out.println(e);
      }
      scan(globalTxId);
    }

  }


  private boolean isGlobalTxAborted(TxEvent event) {
//    String type = event.type();
    String globalTxId = event.globalTxId();

    String key = globalTxId + ":" + EventType.SagaStartedEvent + ":" + globalTxId;

    String status = jedis.hget(key, "status");
    return (status.equals("started") == false);
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
        Map<String, String> map = jedis.hgetAll(gid +":"+EventType.TxStartedEvent+":" + lid);
        if (map.isEmpty() == false) {
          TxEvent eventToCompensate = mapToTxEvent(map);
          Map<String, OmegaCallback> callbacks = omegaCallbacks.get(eventToCompensate.serviceName());
          if (callbacks.isEmpty() == false) {
            OmegaCallback callback = callbacks.get(eventToCompensate.instanceId());
            callback.compensate(eventToCompensate);
          }
        }

        // Remove lid
        // Build GrpcCommand with TxEvent
        // Find Callback, call Callback.compensate
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
