//assembly to package with dependencies ------------------------------

import AssemblyKeys._

assemblySettings

//assembly options

jarName in assembly := "genesis.jar"

//do not run tests for assembly command
test in assembly := {}
