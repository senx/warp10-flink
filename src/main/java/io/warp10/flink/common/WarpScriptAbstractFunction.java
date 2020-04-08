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

package io.warp10.flink.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack.Macro;

public class WarpScriptAbstractFunction implements RichFunction, ListCheckpointed<String>, Serializable {

  public static final String KEY_MACRO = "macro";
  private static final String KEY_SNAPSHOT = "snapshot";
  private static final String KEY_RESTORE = "restore";
  
  private Map<String,Macro> macros = new HashMap<String,Macro>();
  
  private WarpScriptStack stack;

  private RuntimeContext context = null;
  
  private String mc2 = "<% %>";
  
  public WarpScriptAbstractFunction() {
    this.stack = null;
  }

  public void setCode(String code) throws WarpScriptException {
    initStack();
    init(code);
  }
  
  @Override
  public void restoreState(List<String> state) throws Exception {
    if (null == this.macros.get(KEY_RESTORE)) {
      return;
    }
    
    this.stack.clear();
    this.stack.push(state);
    this.stack.exec(this.macros.get(KEY_RESTORE));
  }

  @Override
  public List<String> snapshotState(long checkpointId, long timestamp) throws Exception {
    if (null == this.macros.get(KEY_SNAPSHOT)) {
      return new ArrayList<String>();
    }
    
    this.stack.clear();
    this.stack.exec(this.macros.get(KEY_SNAPSHOT));

    if (1 != this.stack.depth()) {
      throw new Exception("Snapshot macro did not return a single LIST.");
    }

    Object top = this.stack.pop();
    
    if (!(top instanceof List)) {
      throw new Exception("Snapshot macro did not return a LIST.");
    }
    
    List<String> list = new ArrayList<String>(((List) top).size());
    
    for (Object elt: (List) top) {
      if (!(elt instanceof String)) {
        throw new Exception("Snapshot macro did not return a LIST of STRING elements.");        
      }
      list.add((String) elt);
    }
    
    return list;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {    
    out.writeUTF(this.mc2);
  }
  
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    String code = in.readUTF();
    try {
      initStack();
      init(code);    
    } catch (WarpScriptException wse) {
      throw new IOException(wse);
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void initStack() {
    Properties properties = new Properties();
    
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null, properties);
    stack.maxLimits();
    
    this.stack = stack;
  }

  private void init(String code) throws WarpScriptException {
    
    this.macros = new HashMap<String, Macro>();
    
    if (code.startsWith("@")) {
      FileReader reader = null;
      BufferedReader br = null;
      StringBuilder sb = new StringBuilder();
      
      try {
        reader = new FileReader(code.substring(1));
        br = new BufferedReader(reader);
        
        
        while(true) {
          String line = br.readLine();
          
          if (null == line) {
            break;
          }
        
          sb.append(line);
          sb.append("\n");
        }        
      } catch (IOException ioe) {
        throw new WarpScriptException(ioe);
      } finally {
        if (null == br) { try { br.close(); } catch (Exception e) {} }
        if (null == reader) { try { reader.close(); } catch (Exception e) {} }
      }
      
      code = sb.toString();
    }
    
    this.mc2 = code;

    stack.execMulti(code);
    
    Object top = stack.pop();
    
    //
    // WarpScript code should leave a map with the following keys
    //
    // 'macro' Macro to execute for each element
    // 'snapshot' Macro to execute when taking a snapshot as part of checkpointing. This macro should return a list of STRING.
    // 'restore' Macro to execute when restoring a snapshot. It will be called with a list of STRING as produced by 'snapshot'
    //
    // For some implementations (such as Aggregate functions), other keys that 'macro' may be required.
    //
    // If the code returns a macro, it is treated as if the code returned { 'macro' <% ... %> }
    //
    
    if (top instanceof Macro) {
      Map<Object,Object> m = new HashMap<Object,Object>();
      m.put(KEY_MACRO, (Macro) top);
      top = m;
    }
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException("WarpScipt code should return a map.");
    }
    
    Map<Object,Object> map = (Map<Object,Object>) top;
    
    for (Entry<Object,Object> entry: map.entrySet()) {
      if (entry.getKey() instanceof String && entry.getValue() instanceof Macro) {
        this.macros.put(entry.getKey().toString(), (Macro) entry.getValue());
      }
    }    
  }
  
  public WarpScriptStack getStack() {
    return this.stack;
  }
  
  public WarpScriptStack.Macro getMacro() {
    return getMacro(KEY_MACRO);
  }

  public WarpScriptStack.Macro getMacro(String name) {
    return this.macros.get(name);
  }

  @Override
  public IterationRuntimeContext getIterationRuntimeContext() { return null; }
  @Override
  public RuntimeContext getRuntimeContext() { return this.context; }
  @Override
  public void open(Configuration parameters) throws Exception {}
  @Override
  public void setRuntimeContext(RuntimeContext t) { this.context = t; }
  @Override
  public void close() throws Exception {}
}
