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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;

public class FlinkUtils {
  
  public static class GenericTuple extends Tuple {
    
    private Object[] elements = null;
    
    public GenericTuple(int arity) {
      this.elements = new Object[arity];
    }
    
    @Override
    public <T extends Tuple> T copy() {
      Tuple t = new GenericTuple(this.getArity());
      for (int i = 0; i < this.getArity(); i++) {
        t.setField(this.getField(i), i);
      }
      return (T) t;
    }
    
    @Override
    public int getArity() {
      if (null == this.elements) {
        return 0;
      } else {
        return this.elements.length;
      }
    }
    
    @Override
    public <T> T getField(int pos) {
      if (null == this.elements || pos >= this.elements.length) {
        throw new IndexOutOfBoundsException();
      }
      return (T) this.elements[pos];
    }
    
    @Override
    public <T> void setField(T value, int pos) {
      if (null == this.elements) {
        this.elements = new Object[pos];
      } else if (pos >= this.elements.length) {
        this.elements = Arrays.copyOf(this.elements, pos);
      }
      this.elements[pos] = value;
    }    
  }
  
  public static Object fromFlink(Object o) {
    if (null == o) {
      return null;
    } else if (o instanceof String) {
      return o;
    } else if (o instanceof byte[]) {
      return o;
    } else if (o instanceof BigInteger || o instanceof Long || o instanceof Integer || o instanceof Byte) {
      return ((Number) o).longValue();
    } else if (o instanceof BigDecimal || o instanceof Double || o instanceof Float) {
      return ((Number) o).doubleValue();
    } else if (o instanceof Tuple) {
      List<Object> l = new ArrayList<Object>();
      for (int i = 0; i < ((Tuple) o).getArity(); i++) {
        l.add(FlinkUtils.fromFlink(((Tuple) o).getField(i)));
      }
      return l;
    } else if (o instanceof GTSEncoder || o instanceof GeoTimeSerie || o instanceof GTSWrapper) {
      
      GTSWrapper wrapper = null;
      
      if (o instanceof GTSEncoder) {
        wrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper((GTSEncoder) o, true);
      } else if (o instanceof GeoTimeSerie) {
        wrapper = GTSWrapperHelper.fromGTSToGTSWrapper((GeoTimeSerie) o, true);
      } else if (o instanceof GTSWrapper) {
        wrapper = (GTSWrapper) o;
      }
      
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      try {
        byte[] serialized = serializer.serialize(wrapper);
        return serialized;
      } catch (TException te) {
        throw new RuntimeException(te);
      }
    } else {
      throw new RuntimeException("Encountered yet unsupported type: " + o.getClass());
    }
  }
  
  public static Object toFlink(Object o) {
    if (null == o) {
      return null;
    } else if (o instanceof String) {
      return o;
    } else if (o instanceof byte[]) {
      return o;
    } else if (o instanceof Number) {
      return o;
    } else if (o instanceof List) {
      final int arity = ((List) o).size();
      
      Tuple t = new GenericTuple(arity);
      
      for (int i = 0; i < arity; i++) {
        t.setField(toFlink(((List) o).get(i)), i);
      }
      
      return t;
    } else {
      throw new RuntimeException("Encountered yet unsupported type: " + o.getClass());
    }
  }
}
