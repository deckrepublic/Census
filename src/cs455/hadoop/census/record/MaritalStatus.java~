package cs455.hadoop.census.record;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//second of three fields in book record, tracks marital status for male and female
public class MaritalStatus implements Writable{
	private IntWritable [] male;
	private IntWritable [] female;
	
	//Default Constructor, array size determined by census table 
	public MaritalStatus(){
		this.male = new IntWritable[4];
		this.female = new IntWritable[4];
		for(int i = 0; i < this.male.length; i++){
			this.male[i] = new IntWritable();
		}
		for(int i = 0; i < this.female.length; i++){
			this.female[i] = new IntWritable();
		}
	}
	//my constructor
	public MaritalStatus(IntWritable [] male, IntWritable [] female){
		this.male = male;
		this.female = female;
	}
	//Serialize the data fields 
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		for(int i = 0; i < this.male.length; i++){
			this.male[i].write(dataOutput);
		}
		for(int i = 0; i < this.female.length; i++){
			this.female[i].write(dataOutput);
		}
	}
	//de-Serialize the byte stream into data fields
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		for(int i = 0; i < this.male.length; i++){
			this.male[i].readFields(dataInput);
		}
		for(int i = 0; i < this.female.length; i++){
			this.female[i].readFields(dataInput);
		}
	}
	//getters and setters
    public IntWritable [] getMale() {
        return this.male;
    }
    
    public void setPersons(IntWritable [] male) {
        this.male = male;
    }
    
    public IntWritable [] getFemale() {
        return this.female;
    }
    
    public void setFemale(IntWritable [] female) {
        this.female = female;
    }
}
