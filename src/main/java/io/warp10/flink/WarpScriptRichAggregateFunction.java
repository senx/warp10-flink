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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import io.warp10.flink.common.FlinkUtils;
import io.warp10.flink.common.WarpScriptAbstractFunction;
import io.warp10.script.WarpScriptException;

public class WarpScriptRichAggregateFunction<IN,ACC,OUT> extends WarpScriptAbstractFunction implements RichFunction, AggregateFunction<IN,ACC,OUT>, ResultTypeQueryable<OUT> {

  private static final String KEY_ADD = "add";
  private static final String KEY_MERGE = "merge";
  private static final String KEY_RESULT = "result";
  private static final String KEY_CREATE = "create";
  
  private final boolean typeErasure;
  
  public WarpScriptRichAggregateFunction() {
    super();
    typeErasure = false;
  }

  public WarpScriptRichAggregateFunction(String code) throws WarpScriptException {
    super();
    setCode(code);
    typeErasure = true;
  }
  
  @Override
  public ACC add(IN value, ACC accumulator) {
    synchronized(this) {
      try {
        getStack().push(FlinkUtils.fromFlink(accumulator));
        getStack().push(FlinkUtils.fromFlink(value));
        getStack().exec(getMacro(KEY_ADD));
        Object top = getStack().pop();
        
        return (ACC) FlinkUtils.toFlink(top);        
      } catch (WarpScriptException wse) {
        throw new RuntimeException(wse);
      }
    }    
  }
  
  @Override
  public ACC merge(ACC a, ACC b) {
    synchronized(this) {
      try {
        getStack().push(FlinkUtils.fromFlink(a));
        getStack().push(FlinkUtils.fromFlink(b));
        getStack().exec(getMacro(KEY_MERGE));
        Object top = getStack().pop();
        
        return (ACC) FlinkUtils.toFlink(top);
      } catch (WarpScriptException wse) {
        throw new RuntimeException(wse);
      }
    }
  }
  
  @Override
  public OUT getResult(ACC accumulator) {
    synchronized(this) {
      try {
        getStack().push(FlinkUtils.fromFlink(accumulator));
        getStack().exec(getMacro(KEY_RESULT));
        Object top = getStack().pop();
        
        return (OUT) FlinkUtils.toFlink(top);
      } catch (WarpScriptException wse) {
        throw new RuntimeException(wse);
      }
    }
  }
  
  @Override
  public ACC createAccumulator() {
    synchronized(this) {
      try {
        getStack().exec(getMacro(KEY_CREATE));
        Object top = getStack().pop();
        
        return (ACC) FlinkUtils.toFlink(top);      
      } catch (WarpScriptException wse) {
        throw new RuntimeException(wse);
      }      
    }
  }
  
  @Override
  public TypeInformation<OUT> getProducedType() {
    if (typeErasure) {
      return null;
    }
    Type t = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[2];
    return (TypeInformation<OUT>) TypeExtractor.createTypeInfo(t);
  }
}
