setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../../scripts/h2o-r-test-setup.R")
library(MASS)

glmMultinomial <- function() {
  D <- h2o.importFile(locate("bigdata/laptop/glm/multinomial20Class_10KRows.csv")) 
  X <- c(1:h2o.ncol(D)-1)
  Y <- h2o.ncol(D)
  D["C1"] <- h2o.asfactor(D["C1"])
  D["C2"] <- h2o.asfactor(D["C2"])
  D["C3"] <- h2o.asfactor(D["C3"])
  D["C4"] <- h2o.asfactor(D["C4"])
  D["C5"] <- h2o.asfactor(D["C5"])
  D["C6"] <- h2o.asfactor(D["C6"])
  D["C7"] <- h2o.asfactor(D["C7"])
  D["C8"] <- h2o.asfactor(D["C8"])
  D["C9"] <- h2o.asfactor(D["C9"])
  D["C10"] <- h2o.asfactor(D["C10"])
  D["C11"] <- h2o.asfactor(D["C11"])
  D["C12"] <- h2o.asfactor(D["C12"])
  D["C13"] <- h2o.asfactor(D["C13"])
  D["C79"] <- h2o.asfactor(D["C79"])
  
  seeds<-12345
  testF <- h2o.splitFrame(D, c(0.001, 0.89))[[1]] # pick the smaller frame
  multinomialModel <- h2o.glm(y=Y, x=X, training_frame=D, family='multinomial', seed = seeds, solver="COORDINATE_DESCENT",max_iterations=5)
  pred_h2o = h2o.predict(multinomialModel, testF)
  tmpdir_name <- filePath(sandbox(), as.character(Sys.getpid()), fsep=.Platform$file.sep)
  safeSystem(sprintf("rm -fr %s", tmpdir_name))
  safeSystem(sprintf("mkdir -p %s", tmpdir_name))
  h2o.downloadCSV(testF, paste(tmpdir_name, "in.csv", sep="/"))
  outFile = paste(tmpdir_name, "out_mojo.csv", sep="/")
  mojoFile <- locate("bigdata/laptop/glm/GLM_model_python_1543520565753_3.zip")
  a = strsplit(tmpdir_name, '/')
  endIndex <-(which(a[[1]]=="h2o-r"))-1
  genJar <- paste(a[[1]][1:endIndex], collapse='/')
  
  cmd <-
    sprintf(
      "java -ea -cp %s/h2o-assemblies/genmodel/build/libs/genmodel.jar -Xmx12g -XX:ReservedCodeCacheSize=256m hex.genmodel.tools.PredictCsv --mojo %s --input %s/in.csv --output %s/out_mojo.csv --decimal",
      genJar,
      mojoFile,
      tmpdir_name,
      tmpdir_name
    )
  safeSystem(cmd)  # perform mojo prediction
  pred_mojo = h2o.importFile(outFile, header=T)
  compareFrames(pred_h2o, pred_mojo, prob=1, tolerance=1e-6)
}

doTest("GLM: checking multinomial coefficients before and after PUBDEV-5876.", glmMultinomial)
