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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import io.warp10.flink.common.FlinkUtils;
import io.warp10.flink.common.WarpScriptAbstractFunction;
import io.warp10.script.WarpScriptException;

public class WarpScriptRichFoldFunction<O, T> extends WarpScriptAbstractFunction implements RichFunction, FoldFunction<O, T>, ResultTypeQueryable<T> {

  private final boolean typeErasure;
  
  public WarpScriptRichFoldFunction() {
    super();
    typeErasure = false;
  }

  public WarpScriptRichFoldFunction(String code) throws WarpScriptException {
    super();
    setCode(code);
    typeErasure = true;
  }
  
  @Override
  public T fold(T accumulator, O value) throws Exception {
    synchronized(this) {
      getStack().push(FlinkUtils.fromFlink(accumulator));
      getStack().push(FlinkUtils.fromFlink(value));
      getStack().exec(getMacro());
      Object top = getStack().pop();
      
      return (T) FlinkUtils.toFlink(top);
    }    
  }
  
  @Override
  public TypeInformation<T> getProducedType() {
    if (typeErasure) {
      return null;
    }
    Type t = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[1];
    return (TypeInformation<T>) TypeExtractor.createTypeInfo(t);
  }
}
