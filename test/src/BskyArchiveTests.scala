package bskyarchive

import utest.*
import com.mchange.bskyarchive.Tid
import java.time.Instant

object BskyArchiveTests extends TestSuite:
  val tests = Tests {
    test("TID validation") {
      // Valid TID from the user's example
      assert(Tid.isValid("3m42drzpi7k2z"))

      // Valid TIDs
      assert(Tid.isValid("3jzfcijpj2z2a"))
      assert(Tid.isValid("2222222222222"))
      assert(Tid.isValid("7zzzzzzzzzzzy"))

      // Invalid - too short
      assert(!Tid.isValid("3m42drzpi7k2"))

      // Invalid - too long
      assert(!Tid.isValid("3m42drzpi7k2zz"))

      // Invalid - contains invalid first character
      assert(!Tid.isValid("zm42drzpi7k2z"))

      // Invalid - contains uppercase
      assert(!Tid.isValid("3M42drzpi7k2z"))

      // Invalid - contains invalid character
      assert(!Tid.isValid("3m42drzpi7k1z"))
    }

    test("TID parsing") {
      val tidStr = "3m42drzpi7k2z"
      val tid = Tid.parse(tidStr)

      assert(tid.isDefined)
      assert(tid.get.str == tidStr)
      assert(tid.get.toString == tidStr)
    }

    test("TID timestamp extraction") {
      // Parse a known TID and extract its timestamp
      val tid = Tid.parse("3m42drzpi7k2z").get

      // Should have a valid timestamp
      val instant = tid.toInstant
      assert(instant.getEpochSecond > 0)

      // Timestamp should be reasonable (after 2020, before 2030)
      assert(instant.getEpochSecond > 1577836800L) // 2020-01-01
      assert(instant.getEpochSecond < 1893456000L) // 2030-01-01

      // Should have a clock ID between 0-1023
      assert(tid.clockId >= 0)
      assert(tid.clockId < 1024)
    }

    test("TID generation and round-trip") {
      // Generate a TID
      val generated = Tid.generate()

      // Should be valid
      assert(Tid.isValid(generated.str))

      // Parse it back
      val parsed = Tid.parse(generated.str)
      assert(parsed.isDefined)
      assert(parsed.get.str == generated.str)
      assert(parsed.get.value == generated.value)
    }

    test("TID from instant") {
      val instant = Instant.parse("2024-01-15T12:30:45.123456Z")
      val clockId = 42

      val tid = Tid.fromInstant(instant, clockId)

      // Should be valid
      assert(Tid.isValid(tid.str))

      // Should preserve the clock ID
      assert(tid.clockId == clockId)

      // Timestamp should match (within microsecond precision)
      val recovered = tid.toInstant
      assert(recovered.getEpochSecond == instant.getEpochSecond)
      assert(Math.abs(recovered.getNano - instant.getNano) < 1000) // Within 1 microsecond
    }

    test("TID equality") {
      val tid1 = Tid.parse("3m42drzpi7k2z").get
      val tid2 = Tid.parse("3m42drzpi7k2z").get
      val tid3 = Tid.parse("3jzfcijpj2z2a").get

      assert(tid1 == tid2)
      assert(tid1 != tid3)
      assert(tid1.hashCode == tid2.hashCode)
    }

    test("MST entry key reconstruction") {
      import com.mchange.bskyarchive.{MstEntry, Cid}

      // Create a dummy CID for testing
      val dummyCid = Cid(1, 0x71, 0x12, Array[Byte](1, 2, 3, 4))

      // First entry should have prefixLen = 0 and full key in keySuffix
      val entry1 = MstEntry(
        prefixLen = 0,
        keySuffix = "3jzfcijpj2z2a".getBytes("UTF-8"),
        value = dummyCid,
        tree = None
      )

      val key1 = entry1.reconstructKey(Array.empty)
      assert(new String(key1, "UTF-8") == "3jzfcijpj2z2a")

      // Second entry shares first 3 bytes with entry1
      val entry2 = MstEntry(
        prefixLen = 3,
        keySuffix = "abc123xyz".getBytes("UTF-8"),
        value = dummyCid,
        tree = None
      )

      val key2 = entry2.reconstructKey(key1)
      assert(new String(key2, "UTF-8") == "3jzabc123xyz")
    }
  }
