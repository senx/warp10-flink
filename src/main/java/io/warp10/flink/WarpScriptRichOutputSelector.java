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

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import io.warp10.flink.common.FlinkUtils;
import io.warp10.flink.common.WarpScriptAbstractFunction;
import io.warp10.script.WarpScriptException;

public class WarpScriptRichOutputSelector<OUT> extends WarpScriptAbstractFunction implements RichFunction, OutputSelector<OUT> {

  public WarpScriptRichOutputSelector() throws WarpScriptException {
    super();
  }
  
  public WarpScriptRichOutputSelector(String code) throws WarpScriptException {
    super();
    setCode(code);
  }

  @Override
  public Iterable<String> select(OUT value) {
    synchronized(this) {
      try {
        getStack().push(FlinkUtils.fromFlink(value));
        getStack().exec(getMacro());
        Object top = getStack().pop();
      
        if (!(top instanceof List)) {
          throw new RuntimeException("OutputSelector MUST return a list of names.");
        }

        List<String> names = new ArrayList<String>();
        
        for (Object name: (List<Object>) top) {
          names.add(name.toString());
        }
        
        return names;
      } catch (WarpScriptException wse) {
        throw new RuntimeException(wse);
      }
    }    
  }
}
