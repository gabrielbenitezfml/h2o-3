package hex.mojopipeline;

import ai.h2o.mojos.runtime.frame.*;
import ai.h2o.mojos.runtime.MojoPipeline;
import ai.h2o.mojos.runtime.utils.SimpleCSV;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import water.Scope;
import water.TestUtil;
import water.fvec.ByteVec;
import water.fvec.Frame;
import water.fvec.Vec;
import water.parser.ParseSetup;

import java.io.*;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class MojoPipelineTest extends TestUtil {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Parameterized.Parameter
  public String testCase;

  private String dataFile;
  private String mojoFile;

  @Parameterized.Parameters(name="{0}")
  public static Object[] data() {
    String testDir = System.getenv("MOJO_PIPELINE_TEST_DIR");
    if (testDir == null) {
      return new Object[0];
    }
    return new File(testDir).list();
  }

  @BeforeClass
  public static void stall() {
    stall_till_cloudsize(1);
  }

  @Before
  public void checkLicense() {
    Assume.assumeNotNull(System.getenv("DRIVERLESS_AI_LICENSE_FILE"));
  }

  @Before
  public void extractData() throws IOException  {
    String testDir = System.getenv("MOJO_PIPELINE_TEST_DIR");
    Assume.assumeNotNull(testDir);

    //final String dir = "test_mojo_credit_binary_1";
    //String dir = "test_mojo_weather_binary_linear";
    System.out.println(testCase);
    File source = new File(new File(testDir, testCase), "mojo.zip");
    File target = tmp.newFolder(testCase);
    extractZip(source, target);
    dataFile = new File(new File(target, "mojo-pipeline"), "example.csv").getAbsolutePath();
    mojoFile = new File(new File(target, "mojo-pipeline"), "pipeline.mojo").getAbsolutePath();
  }

  @Test
  public void transform() {
    try {
      Scope.enter();
      // get the expected data
      MojoPipeline model = null;
      MojoFrame expected = null;
      try {
        model = MojoPipeline.loadFrom(mojoFile);
        expected = transformDirect(model, dataFile);
      } catch (Exception e) {
        Assume.assumeNoException(e);
      }

      ByteVec mojoData = makeNfsFileVec(mojoFile);
      final MojoFrameMeta meta = model.getInputMeta();
      Frame t = Scope.track(parse_test_file(dataFile, new ParseSetupTransformer() {
        @Override
        public ParseSetup transformSetup(ParseSetup guessedSetup) {
          byte[] columnTypes = guessedSetup.getColumnTypes();
          for (int i = 0; i < meta.size(); i++) {
            if ((columnTypes[i] == Vec.T_NUM) && ! meta.getColumnType(i).isnumeric) {
              columnTypes[i] = Vec.T_STR;
            }
            if ((columnTypes[i] == Vec.T_TIME) && meta.getColumnType(i) != MojoColumn.Type.Time64) {
              columnTypes[i] = Vec.T_STR;
            }
          }
          return guessedSetup;
        }
      }));

      hex.mojopipeline.MojoPipeline mp = new hex.mojopipeline.MojoPipeline(mojoData);
      Frame transformed = mp.transform(t, false);

      System.out.println(transformed.toTwoDimTable().toString());
      assertFrameEquals(transformed, expected);
    } finally {
      Scope.exit();
    }
  }

  private void assertFrameEquals(Frame actual, MojoFrame expected) {
    assertArrayEquals(actual.names(), expected.getColumnNames());
    for (int i = 0; i < expected.getNcols(); i++) {
      double[] vals = (double[]) expected.getColumn(i).getData();
      Vec expectedVec = Scope.track(dvec(vals));
      assertVecEquals(expectedVec, actual.vec(i), 1e-6);
    }
  }

  private MojoFrame transformDirect(MojoPipeline model, String dataPath) throws Exception {
    SimpleCSV csv =  SimpleCSV.read(dataPath);
    MojoFrameBuilder fb = model.getInputFrameBuilder();
    String[] labels = csv.getLabels();
    String[][] data = csv.getData();
    MojoRowBuilder rb = fb.getMojoRowBuilder();
    for (String[] row : data) {
      for (int i = 0; i < row.length; i += 1)
        rb.setValue(labels[i], row[i]);
      rb = fb.addRow(rb);
    }
    MojoFrame input = fb.toMojoFrame();
    return model.transform(input);
  }

  private static void extractZip(File source, File target) throws IOException {
    try (ZipFile zipFile = new ZipFile(source)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        File entryDestination = new File(target,  entry.getName());
        if (entry.isDirectory()) {
          if (! entryDestination.mkdirs()) {
            throw new IOException("Failed to create directory: " + entryDestination);
          }
        } else {
          if (! entryDestination.getParentFile().exists() && ! entryDestination.getParentFile().mkdirs()) {
            throw new IOException("Failed to create directory: " + entryDestination.getParentFile());
          }
          try (InputStream in = zipFile.getInputStream(entry);
               OutputStream out = new FileOutputStream(entryDestination)) {
            IOUtils.copy(in, out);
          }
        }
      }
    }
  }

}
