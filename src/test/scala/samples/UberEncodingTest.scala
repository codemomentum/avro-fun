package samples

import java.io.File

import com.miguno.kafka.avro.{AvroDecoderWithRegistry, AvroEncoder}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.junit.Assert._
import org.scalatest.{BeforeAndAfter, FunSuite}

class UberEncodingTest extends FunSuite with BeforeAndAfter {

  var uber: Schema = _
  var log1: Schema = _
  var log2: Schema = _
  var encoder: AvroEncoder[GenericRecord] = _
  var log1Decoder: AvroDecoderWithRegistry[GenericRecord] = _
  var log2Decoder: AvroDecoderWithRegistry[GenericRecord] = _

  before {

    val p0 = new Schema.Parser
    uber = p0.parse(new File("src/main/avro/union/uber.avsc"))

    val p1 = new Schema.Parser
    log1 = p1.parse(new File("src/main/avro/union/log1.avsc"))

    val p2 = new Schema.Parser
    log2 = p2.parse(new File("src/main/avro/union/log2.avsc"))

    encoder = new AvroEncoder[GenericRecord](null, uber)
    log1Decoder = new AvroDecoderWithRegistry[GenericRecord](null, uber, log1)
    log2Decoder = new AvroDecoderWithRegistry[GenericRecord](null, uber, log2)
  }

  test("test create generic log and extract 2 different views" +
    "its possible to create a 'view' on top of an uber schema") {

    val uberlog = new GenericData.Record(uber)
    uberlog.put("log1_field1", 31)
    uberlog.put("log1_field2", new Utf8("Sheldon1"))
    uberlog.put("log2_field1", 72)
    uberlog.put("log2_field2", new Utf8("Sheldon2"))

    val serialized: Array[Byte] = encoder.toBytes(uberlog)


    val log1Val: GenericRecord = log1Decoder.fromBytes(serialized)
    assertEquals(uberlog.get("log1_field1"), log1Val.get("log1_field1"))
    assertEquals(uberlog.get("log1_field2"), log1Val.get("log1_field2"))


    val log2Val: GenericRecord = log2Decoder.fromBytes(serialized)
    assertEquals(uberlog.get("log2_field1"), log2Val.get("log2_field1"))
    assertEquals(uberlog.get("log2_field2"), log2Val.get("log2_field2"))
  }

}
