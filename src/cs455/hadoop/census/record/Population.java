package cs455.hadoop.census.record;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//first of three fields in book record, population tracks population numbers
public class Population implements Writable{
	private IntWritable persons;
	private IntWritable [] urban_rural;
	private IntWritable [] sex;
	private IntWritable [] age;
	private IntWritable [][] age_by_gender;
	
	//Default Constructor, array size determined by census table 
	public Population(){
		this.persons = new IntWritable();
		this.urban_rural = new IntWritable[2];
		this.sex = new IntWritable[2];
		this.age = new IntWritable[31];
		this.age_by_gender = new IntWritable[2][31];
		for(int i = 0; i < this.urban_rural.length; i++){
			this.urban_rural[i] = new IntWritable();
		}
		for(int i = 0; i < this.sex.length; i++){
			this.sex[i] = new IntWritable();
		}
		for(int i = 0; i < this.age.length; i++){
			this.age[i] = new IntWritable();
		}
		for(int i = 0; i < this.age_by_gender.length; i++){
			for(int j = 0; j < this.age_by_gender[i].length; j++){
				this.age_by_gender[i][j] = new IntWritable();
			}
		}
	}
	//my constructor
	public Population(IntWritable persons, IntWritable [] urban_rural, 
			IntWritable [] sex, IntWritable [] age, IntWritable [][] age_by_gender){
		this.persons = persons;
		this.urban_rural = urban_rural;
		this.sex = sex;
		this.age = age;
		this.age_by_gender = age_by_gender;
	}
	//Serialize the data fields 
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.persons.write(dataOutput);
		for(int i = 0; i < this.urban_rural.length; i++){
			this.urban_rural[i].write(dataOutput);
		}
		for(int i = 0; i < this.sex.length; i++){
			this.sex[i].write(dataOutput);
		}
		for(int i = 0; i < this.age.length; i++){
			this.age[i].write(dataOutput);
		}
		for(int i = 0; i < this.age_by_gender.length; i++){
			for(int j = 0; j < this.age_by_gender[i].length; j++){
				this.age_by_gender[i][j].write(dataOutput);
			}
		}
	}
	//de-Serialize the byte stream into data fields
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.persons.readFields(dataInput);
		for(int i = 0; i < this.urban_rural.length; i++){
			IntWritable read = this.urban_rural[i];
			read.readFields(dataInput);
		}
		for(int i = 0; i < this.sex.length; i++){
			this.sex[i].readFields(dataInput);
		}
		for(int i = 0; i < this.age.length; i++){
			this.age[i].readFields(dataInput);
		}
		for(int i = 0; i < this.age_by_gender.length; i++){
			for(int j = 0; j < this.age_by_gender[i].length; j++){
				this.age_by_gender[i][j].readFields(dataInput);
			}
		}
	}
	//getters and setters
    public IntWritable getPersons() {
        return this.persons;
    }
    
    public void setPersons(IntWritable persons) {
        this.persons = persons;
    }
    
    public IntWritable [] getUrbanRural() {
        return this.urban_rural;
    }
    
    public void setUrbanRural(IntWritable [] urban_rural) {
        this.urban_rural = urban_rural;
    }
    
    public IntWritable [] getSex() {
        return this.sex;
    }
    
    public void setSex(IntWritable [] sex) {
        this.sex = sex;
    }
    
    public IntWritable [] getAge() {
        return this.age;
    }
    
    public void setAge(IntWritable [] age) {
        this.age = age;
    }
    
    public IntWritable [][] getAgeByGender() {
        return this.age_by_gender;
    }
    
    public void setAgeByGender(IntWritable [][] age_by_gender) {
        this.age_by_gender = age_by_gender;
    }
    
}
