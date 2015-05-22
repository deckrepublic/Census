package cs455.hadoop.census.record;

import org.apache.hadoop.io.*;
import java.io.*;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BookRecord implements Writable, WritableComparable<BookRecord>{
	
	private Text key;
	private IntWritable logical_record;
	private BookData data;
	
	//Default Constructor
	public BookRecord(){
		this.key = new Text();
		this.logical_record = new IntWritable();
		this.data = new BookData();
	}
	
	//my constructor
	public BookRecord(Text key, IntWritable logicalRecord, BookData data){
		this.key = key;
		this.logical_record = logicalRecord;
		this.data = data;
	}
	//Serialize the data fields 
	@Override	
	public void write(DataOutput dataOutput) throws IOException {
		this.key.write(dataOutput);
		this.logical_record.write(dataOutput);
		this.data.write(dataOutput);
	}
	//de-Serialize the byte stream into data fields
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.key.readFields(dataInput);
		this.logical_record.readFields(dataInput);
		this.data.readFields(dataInput);
	}
	//the compare function so that hadoop will sort records based on state name
	@Override
	public int compareTo(BookRecord o){
	        return this.getKey().compareTo(o.getKey());
	}
        @Override   
        public boolean equals(Object o){

		if (o instanceof BookRecord){
			BookRecord other = (BookRecord) o;
			return key.equals(other.getKey());
		}
		return false;
	}
	//getters and setters
    public Text getKey() {
        return this.key;
    }
    
    public void setKey(Text key) {
        this.key = key;
    }
    
    public IntWritable getLogicalRecord() {
        return this.logical_record;
    }
    
    public void setLogicalRecord(IntWritable logical_record) {
        this.logical_record = logical_record;
    }
    
    public BookData getData() {
        return this.data;
    }
    
    public void setData(BookData data) {
        this.data = data;
    }
}
