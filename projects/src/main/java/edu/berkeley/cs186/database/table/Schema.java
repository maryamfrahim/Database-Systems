package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.databox.*;

import javax.xml.crypto.Data;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The Schema of a particular table.
 *
 * Properties:
 * `fields`: an ordered list of column names
 * `fieldTypes`: an ordered list of data types corresponding to the columns
 * `size`: physical size (in bytes) of a record conforming to this schema
 */
public class Schema {
  private List<String> fields;
  private List<DataBox> fieldTypes;
  private int size;

  public Schema(List<String> fields, List<DataBox> fieldTypes) {
    assert(fields.size() == fieldTypes.size());

    this.fields = fields;
    this.fieldTypes = fieldTypes;
    this.size = 0;

    for (DataBox dt : fieldTypes) {
      this.size += dt.getSize();
    }
  }

  /**
   * Verifies that a list of DataBoxes corresponds to this schema. A list of
   * DataBoxes corresponds to this schema if the number of DataBoxes in the
   * list equals the number of columns in this schema, and if each DataBox has
   * the same type and size as the columns in this schema.
   *
   * @param values the list of values to check
   * @return a new Record with the DataBoxes specified
   * @throws SchemaException if the values specified don't conform to this Schema
   */
  public Record verify(List<DataBox> values) throws SchemaException {
    if (values.size() == this.fields.size()) {
      for (int i = 0; i < values.size(); i++) {
        if (values.get(i).type() == this.fieldTypes.get(i).type()) {
          if (values.get(i).getSize() == this.fieldTypes.get(i).getSize()) {
            return new Record(values);
          }
        }
      }
    }
    SchemaException expection = new SchemaException ("Dishonor on you. Dishonor on your cow. Also Values specified don't conform to this Schema");
    throw expection;
    //return null;
  }

  /**
   * Serializes the provided record into a byte[]. Uses the DataBoxes'
   * serialization methods. A serialized record is represented as the
   * concatenation of each serialized DataBox. This method assumes that the
   * input record corresponds to this schema.
   *
   * @param record the record to encode
   * @return the encoded record as a byte[]
   */
  public byte[] encode(Record record) {
    // TODO: implement me! use java.nio.ByteBuffer
    ByteBuffer trial = ByteBuffer.allocate(this.size);
    for (DataBox data : record.getValues()) {
      byte[] item = data.getBytes();
      trial.put(item);
    }
    return trial.array();
  }

  /**
   * Takes a byte[] and decodes it into a Record. This method assumes that the
   * input byte[] represents a record that corresponds to this schema.
   *
   * @param input the byte array to decode
   * @return the decoded Record
   */
  public Record decode(byte[] input) {
    // TODO: implement me! use Arrays.copyOfRange which copies the specified range of the specified array into a new array

//    go though the this.feildtype
//            get type and size
//            and make a new databox and add it to a list
//            return the list
    int counter = 0;
    List<DataBox> returnable = new ArrayList<DataBox>();
    for (DataBox item : this.fieldTypes) {  //fieldtypes
      int lenn = item.getSize();
      if(item.type().equals(DataBox.Types.BOOL)){
        byte[] heyy1 = Arrays.copyOfRange(input, counter, counter+lenn);
        BoolDataBox what = new BoolDataBox(heyy1);
        returnable.add(what);
      }
      if(item.type().equals(DataBox.Types.valueOf("FLOAT"))){
        byte[] heyy2 = Arrays.copyOfRange(input, counter, counter+lenn);
        FloatDataBox whatt = new FloatDataBox(heyy2);
        returnable.add(whatt);
      }
      if(item.type().equals(DataBox.Types.valueOf("INT"))){
        byte[] heyy3 = Arrays.copyOfRange(input, counter, counter+lenn);
        IntDataBox whattt = new IntDataBox(heyy3);
        returnable.add(whattt);
      }
      if(item.type().equals(DataBox.Types.valueOf("STRING"))){
        byte[] heyy4 = Arrays.copyOfRange(input, counter, counter+lenn);
        StringDataBox whatttt = new StringDataBox(heyy4);
        returnable.add(whatttt);
      }
      counter += lenn;
    }
    return new Record(returnable);
  }
//
//    int el = input.length;
//    int counter = 0;
//    DataBox help = input.getClass();
//    for (int i = 0; i < input.length; i++) {
//      int current = i.getSize();
//      int bluff = 5;
//      DataBox hey = new DataBox(bluff);
//      if(item.type() == DataBox.Types.BOOL){
//        i.getsize();
//        byte[] buf = Arrays.copyOfRange(input, i, i+1);
//        DataBox segment = buf.DataBox();
//      }
//      if(Databox i == FloatDataBox){
//        Arrays.copyOfRange(input, i, i+8);
//      }
//      if(Databox i == IntDataBox){
//        Arrays.copyOfRange(input, i, i+8);
//      }
//      if(Databox i == StringDataBox){
//        Arrays.copyOfRange(input, i, i+i.size());
//      }
//    }


  public int getEntrySize() {
    return this.size;
  }

  public List<String> getFieldNames() {
    return this.fields;
  }

  public List<DataBox> getFieldTypes() {
    return this.fieldTypes;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Schema)) {
      return false;
    }

    Schema otherSchema = (Schema) other;

    if (this.fields.size() != otherSchema.fields.size()) {
      return false;
    }

    for (int i = 0; i < this.fields.size(); i++) {
      DataBox thisType = this.fieldTypes.get(i);
      DataBox otherType = otherSchema.fieldTypes.get(i);

      if (thisType.type() != otherType.type()) {
        return false;
      }

      if (thisType.type().equals(DataBox.Types.STRING) && thisType.getSize() != otherType.getSize()) {
        return false;
      }
    }

    return true;
  }
}
