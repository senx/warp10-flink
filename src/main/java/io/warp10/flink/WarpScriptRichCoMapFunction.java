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

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import io.warp10.flink.common.FlinkUtils;
import io.warp10.flink.common.WarpScriptAbstractFunction;
import io.warp10.script.WarpScriptException;

public class WarpScriptRichCoMapFunction<IN1, IN2, OUT> extends WarpScriptAbstractFunction implements RichFunction, CoMapFunction<IN1, IN2, OUT>, ResultTypeQueryable<OUT> {

  private final boolean typeErasure;
  
  public WarpScriptRichCoMapFunction() {
    super();
    typeErasure = false;
  }

  public WarpScriptRichCoMapFunction(String code) throws WarpScriptException {
    super();
    setCode(code);
    typeErasure = true;
  }
  
  @Override
  public OUT map1(IN1 value) throws Exception {
    synchronized(this) {
      getStack().push(1);
      getStack().push(FlinkUtils.fromFlink(value));
      getStack().exec(getMacro());
      Object top = getStack().pop();
      
      return (OUT) FlinkUtils.toFlink(top);
    }
  }
  
  @Override
  public OUT map2(IN2 value) throws Exception {
    synchronized(this) {
      getStack().push(2);
      getStack().push(FlinkUtils.fromFlink(value));
      getStack().exec(getMacro());
      Object top = getStack().pop();
      
      return (OUT) FlinkUtils.toFlink(top);
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
