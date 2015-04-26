package samples

import java.io.File

import com.miguno.kafka.avro.{AvroDecoder, AvroDecoderWithRegistry, AvroEncoder}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.Assert._


class EncodingTest extends FunSuite with BeforeAndAfter {

  var s1: Schema = _
  var s2: Schema = _
  var encoder: AvroEncoder[GenericRecord] = _
  var v2decoder: AvroDecoder[GenericRecord] = _
  var v2decoderWithReg: AvroDecoderWithRegistry[GenericRecord] = _

  before {
    val p1 = new Schema.Parser
    s1 = p1.parse(new File("src/main/avro/v1/v1.avsc"))

    val p2 = new Schema.Parser
    s2 = p2.parse(new File("src/main/avro/v2/v2.avsc"))

    encoder = new AvroEncoder[GenericRecord](null, s1)
    v2decoder = new AvroDecoder[GenericRecord](null, s2)
    v2decoderWithReg = new AvroDecoderWithRegistry[GenericRecord](null, s1, s2)
  }



  test("create some data with v1") {
    val person = new GenericData.Record(s1)
    person.put("age", 31)
    person.put("name", new Utf8("Sheldon"))
    person.put("favorite_number", 72)
    person.put("favorite_color", new Utf8("Red"))

    val serialized: Array[Byte] = encoder.toBytes(person)

    try {
      val deSerialized: GenericRecord = v2decoder.fromBytes(serialized)
      assertEquals(person.get("name"), deSerialized.get("name"))
      assertEquals(person.get("age"), deSerialized.get("age"))
      assertEquals(person.get("favorite_color"), deSerialized.get("favorite_color"))
      assertEquals(person.get("favorite_number"), deSerialized.get("favorite_number"))
    }
    catch {
      case _: Exception => println("v1 failed")
    }

    val deSerialized2: GenericRecord = v2decoderWithReg.fromBytes(serialized)
    assertEquals(person.get("name"), deSerialized2.get("name"))
    assertEquals(person.get("age"), deSerialized2.get("age"))
    assertEquals(person.get("favorite_color"), deSerialized2.get("favorite_color"))
    assertEquals(person.get("favorite_number"), deSerialized2.get("favorite_number"))


  }

}
