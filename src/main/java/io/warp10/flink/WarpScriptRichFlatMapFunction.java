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
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import io.warp10.flink.common.FlinkUtils;
import io.warp10.flink.common.WarpScriptAbstractFunction;
import io.warp10.script.WarpScriptException;

public class WarpScriptRichFlatMapFunction<IN, OUT> extends WarpScriptAbstractFunction implements RichFunction, FlatMapFunction<IN, OUT> , ResultTypeQueryable<OUT> {

  private final boolean typeErasure;
  
  public WarpScriptRichFlatMapFunction() {
    super();
    typeErasure = false;
  }

  public WarpScriptRichFlatMapFunction(String code) throws WarpScriptException {
    super();
    setCode(code);
    typeErasure = true;
  }

  @Override
  public void flatMap(IN value, org.apache.flink.util.Collector<OUT> out) throws Exception {
    synchronized(this) {
      getStack().push(FlinkUtils.fromFlink(value));
      getStack().exec(getMacro());
      Object top = getStack().pop();
      
      if (!(top instanceof List)) {
        throw new WarpScriptException("Invalid return value, MUST be a list.");
      }
      
      List<Object> l = (List<Object>) top;
      
      for (Object o: l) {
        out.collect((OUT) FlinkUtils.toFlink(o));
      }      
    }
  }
  
  @Override
  public TypeInformation<OUT> getProducedType() {
    if (typeErasure) {
      return null;
    }
    Type t = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    return (TypeInformation<OUT>) TypeExtractor.createTypeInfo(t);
  }
}
