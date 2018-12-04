package hex.mojopipeline;

import ai.h2o.mojos.runtime.frame.*;
import ai.h2o.mojos.runtime.lic.LicenseException;
import ai.h2o.mojos.runtime.readers.MojoPipelineReaderBackendFactory;
import ai.h2o.mojos.runtime.readers.MojoReaderBackend;
import ai.h2o.mojos.runtime.frame.MojoColumn.Type;
import water.*;
import water.fvec.*;
import water.parser.BufferedString;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class MojoPipeline extends Iced<MojoPipeline> {

  private ByteVec _mojoData;
  private transient ai.h2o.mojos.runtime.MojoPipeline _mojoPipeline;

  public MojoPipeline(ByteVec mojoData) {
    _mojoData = mojoData;
    _mojoPipeline = readPipeline(_mojoData);
  }

  public Frame transform(Frame f, boolean allowTimestamps) {
    Frame adaptedFrame = adaptFrame(f, allowTimestamps);
    byte[] types = outputTypes();
    return new MojoPipelineTransformer(_mojoData._key).doAll(types, adaptedFrame)
            .outputFrame(null, _mojoPipeline.getOutputMeta().getColumnNames(), null);
  }

  private byte[] outputTypes() {
    MojoFrameMeta outputMeta = _mojoPipeline.getOutputMeta();
    for (Type type : outputMeta.getColumnTypes()) {
      if (! type.isfloat) {
        throw new UnsupportedOperationException("Output type " + type.name() + " is not supported.");
      }
    }
    byte[] types = new byte[outputMeta.size()];
    Arrays.fill(types, Vec.T_NUM);
    return types;
  }

  private Frame adaptFrame(Frame f, boolean allowTimestamps) {
    return adaptFrame(f, _mojoPipeline.getInputMeta(), allowTimestamps);
  }

  private static Frame adaptFrame(Frame f, MojoFrameMeta inputMeta, boolean allowTimestamps) {
    String[] colNames = inputMeta.getColumnNames();
    Frame adaptedFrame = new Frame();
    for (String name : colNames) {
      Vec v = f.vec(name);
      if (v == null) {
        throw new IllegalArgumentException("Input frame is missing a column: " + name);
      }
      if (v.get_type() == Vec.T_BAD || v.get_type() == Vec.T_UUID) {
        throw new UnsupportedOperationException("Columns of type " + v.get_type_str() + " are currently not supported.");
      }
      if (! allowTimestamps && v.get_type() == Vec.T_TIME && inputMeta.getColumnType(name) == Type.Str) {
        throw new IllegalArgumentException("MOJO Pipelines currently do not support datetime columns represented as timestamps. " +
                "Please parse your dataset again and make sure column '" + name + "' is parsed as String instead of Timestamp. " +
                "You can also enable implicit timestamp conversion in your client. Please refer to documentation of the transform function.");
      }
      adaptedFrame.add(name, v);
    }
    return adaptedFrame;
  }

  private static ai.h2o.mojos.runtime.MojoPipeline readPipeline(ByteVec mojoData) {
    try {
      try (InputStream input = mojoData.openStream(null);
           MojoReaderBackend reader = MojoPipelineReaderBackendFactory.createReaderBackend(input)) {
        return ai.h2o.mojos.runtime.MojoPipeline.loadFrom(reader);
      }
    } catch (IOException | LicenseException e) {
      throw new RuntimeException(e);
    }
  }

  private static class MojoPipelineTransformer extends MRTask<MojoPipelineTransformer> {

    private final Key<Vec> _mojoDataKey;
    private transient ai.h2o.mojos.runtime.MojoPipeline _pipeline;

    private MojoPipelineTransformer(Key<Vec> mojoDataKey) {
      _mojoDataKey = mojoDataKey;
    }

    @Override
    protected void setupLocal() {
      ByteVec mojoData = DKV.getGet(_mojoDataKey);
      _pipeline = readPipeline(mojoData);
    }

    @Override
    public void map(Chunk[] cs, NewChunk[] ncs) {
      SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy'T'hh:mm:ss.sss");
      assert cs.length == _pipeline.getInputMeta().size();
      MojoFrameBuilder frameBuilder = _pipeline.getInputFrameBuilder();
      MojoRowBuilder rowBuilder = frameBuilder.getMojoRowBuilder();
      for (int i = 0; i < cs[0]._len; i++) {
        for (int col = 0; col < cs.length; col++) {
          final Type type = _pipeline.getInputMeta().getColumnType(col);
          Chunk c = cs[col];
          if (c.isNA(i))
            continue;
          switch (c.vec().get_type()) {
            case Vec.T_NUM:
              double val = c.atd(i);
              if (type == Type.Str) {
                rowBuilder.setString(col, String.valueOf(val));
              } else {
                rowBuilder.setDouble(col, val);
              }
              break;
            case Vec.T_CAT:
              rowBuilder.setValue(col, c.vec().domain()[(int) c.at8(i)]);
              break;
            case Vec.T_STR:
              rowBuilder.setString(col, c.atStr(new BufferedString(), i).toString());
              break;
            case Vec.T_TIME:
              final long timestamp = c.at8(i);
              if (type == Type.Time64) {
                rowBuilder.setTimestamp(col, new Timestamp(timestamp));
              } else {
                rowBuilder.setValue(col, df.format(new Date(timestamp)));
              }
              break;
            default:
              throw new IllegalStateException("Unexpected column type: " + c.vec().get_type_str());
          }
        }
        frameBuilder.addRow(rowBuilder);
      }
      MojoFrame transformed = _pipeline.transform(frameBuilder);
      for (int col = 0; col < ncs.length; col++) {
        NewChunk nc = ncs[col];
        MojoColumn column = transformed.getColumn(col);
        assert column.size() == cs[0].len();
        double[] data = (double[]) column.getData();
        for (double d : data) {
          nc.addNum(d);
        }
      }
    }
  }

}
