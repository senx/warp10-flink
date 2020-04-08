//
//   Copyright 2016-2020  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.flink;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.text.ParseException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.thrift.data.Metadata;

@WebSocket
public class PlasmaFlinkSource extends RichSourceFunction<Object> {

  private final AtomicBoolean abort = new AtomicBoolean(false);
  
  private final String endpoint;
  
  private CountDownLatch closeLatch;

  private Session session = null;

  private final AtomicBoolean done = new AtomicBoolean(false);

  private final String token;
  
  private final String[] selectors;
  
  private final long period;
  
  private SourceContext ctx = null;
  
  private AtomicLong count = new AtomicLong(0L);
  
  public PlasmaFlinkSource(String endpoint, String token, long period, String... selectors) {    
    this.endpoint = endpoint;
    this.token = token;
    this.selectors = selectors;
    this.period = period;
  }
  
  @Override
  public void cancel() {
    this.abort.set(true);
  }
  
  @Override
  public void run(SourceContext<Object> ctx) throws Exception {
    
    this.ctx = ctx;
    
    while(true) {
      WebSocketClient client = null;
      
      try {
        SslContextFactory ssl = new SslContextFactory();
        client = new WebSocketClient(ssl);
        client.start();
        URI uri = new URI(this.endpoint);
        ClientUpgradeRequest request = new ClientUpgradeRequest();
        
        this.closeLatch = new CountDownLatch(1);
        this.session = null;
        
        Future<Session> future = client.connect(this, uri, request);
        
        //
        // Wait until we're connected to the endpoint
        //
        
        while(!future.isDone()) {
          LockSupport.parkNanos(250000000L);
        }

        //
        // Wait until we're told to exit or the socket has closed
        //
        
        long lastSub = 0L;

        this.session.getRemote().sendString("WRAPPER");
        
        while(true) {
          //
          // Every now and then re-issue 'SUBSCRIBE' statements to the endpoint
          //
          if (null == this.session || this.done.get()) {
            return;
          }
          
          if (System.currentTimeMillis() - lastSub > this.period) {
            for (String selector: selectors) {
              this.session.getRemote().sendString("SUBSCRIBE " + this.token + " " + selector);
            }
            lastSub = System.currentTimeMillis();
          }
          
          LockSupport.parkNanos(1000000000L);
        }
      } catch (Exception e) {
        e.printStackTrace();
        try { this.awaitClose(1, TimeUnit.SECONDS); } catch (InterruptedException ie) {}
      } finally {
        try { client.stop(); } catch (Throwable t) {}
      }
    }
  }

  public void subscribe(String token, String selector) { 
  }
  
  public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
    return this.closeLatch.await(duration, unit);
  }

  @OnWebSocketClose
  public void onClose(int statusCode, String reason) {
    this.session = null;
    this.closeLatch.countDown();
  }
 
  @OnWebSocketConnect
  public void onConnect(Session session) {
    this.session = session;
  }
 
  @OnWebSocketMessage
  public void onMessage(String msg) {
    BufferedReader br = null;
    
    try {
      br = new BufferedReader(new StringReader(msg));      
      
      GTSEncoder lastencoder = null;
      GTSEncoder encoder = null;
      
      while(true) {
        String line = br.readLine();
        
        if (null == line) {
          break;
        }
        
        long ts = Long.parseLong(line.substring(0, line.indexOf("/")));
      
        try {
          encoder = GTSHelper.parse(lastencoder, line);
        } catch (ParseException pe) {
          // Emit current encoder
          if (null != lastencoder) {
            ctx.collect(lastencoder);
            
            //
            // Allocate a new GTSEncoder and reuse Metadata so we can
            // correctly handle a continuation line if this is what occurs next
            //
            Metadata metadata = lastencoder.getMetadata();
            lastencoder = new GTSEncoder(0L);
            lastencoder.setMetadata(metadata);
          }
        }
        
        if (lastencoder != null && lastencoder != encoder) {
          ctx.collect(lastencoder);
          lastencoder = encoder;
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    } finally {
      if (null != br) {
        try {
          br.close();
        } catch (Exception e) {          
        }
      }
    }
  }    
}
