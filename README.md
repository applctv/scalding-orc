scalding-orc
============

This project provides read and write support for [ORC file format](https://orc.apache.org/) in [Scalding](https://gihub.com/twitter/scalding/).

[![Build Status](https://travis-ci.org/applctv/scalding-orc.svg?branch=master)](https://travis-ci.org/applctv/scalding-orc)

# Basics
Define a case class with schema matching that of your source or sink. Member names should match the column names in the schema, and their types should correspond.
Nested schemas as well as Arrays/Lists and Maps are supported.

```
// Import implicit macro conversions
import io.applicative.scalding.orc.MacroImplicits._

// Define your record as a case class
case class ReadSample(boolean1: Boolean, byte1: Byte, short1: Short, int1: Int, long1: Long)

// Read:
val myPipe = TypedPipe.from(TypedOrc[ReadSample]("/path/to/file.orc"))
// Write:
myPipe.write(TypedOrc[ReadSample](outputPath))
```

# Column pruning
To eliminate unneeded columns, only define the relevant fields in your case class. Make sure to match the column names. Orc Reader will skip unneded columns, improving IO performance.

# Predicate pushdown

Note: predicate pushdown is a hint to the Orc Reader to skip some rows, but is not a strict filter. See [this issue](https://github.com/HotelsDotCom/corc/issues/12) for more details.

```
val fp = org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactoy.newBuilder
  .startAnd.equals("columnname", "value").end.build()
val myPipe = TypedPipe.from(TypedOrc[ReadSample]("/path/to/file.orc", fp))

```

# Common issues

## Failed to generate proper converter/setter

This occurs when the macro for your case class couldn't be generated. Check the case class member types, compile with "-Xlog-implicits" flag, and look for 'materializeCaseClassTupleSetter' and 'materializeCaseClassTupleConverter'. If you can't spot the error, file an issue with your case class implementation.

## readTypeInfo [...] does not match actualTypeInfo [...]

The schema of the file doesn't match the schema specified by your case class. Double check column names and types.
