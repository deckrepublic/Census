package cs455.hadoop.census.record;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BookData implements Writable{
	
	private Population population;
	private MaritalStatus marital_status;
	private Tenure tenure;
	
	//Default Constructor
	public BookData(){
		this.population = new Population();
		this.marital_status = new MaritalStatus();
		this.tenure = new Tenure();
	}
	//my constructor
	public BookData(Population population, MaritalStatus marital_status, Tenure tenure){
		this.population = population;
		this.marital_status = marital_status;
		this.tenure = tenure;
	}
	//Serialize the data fields 
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.population.write(dataOutput);
		this.marital_status.write(dataOutput);
		this.tenure.write(dataOutput);
	}
	//de-Serialize the byte stream into data fields
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.population.readFields(dataInput);
		this.marital_status.readFields(dataInput);
		this.tenure.readFields(dataInput);
	}
	//getters and setters
    public Population getPopulation() {
        return this.population;
    }
    
    public void setPopulation(Population population) {
        this.population = population;
    }
    
    public MaritalStatus getMaritalStatus() {
        return this.marital_status;
    }
    
    public void setMaritalStatus(MaritalStatus marital_status) {
        this.marital_status = marital_status;
    }
    
    public Tenure getTenure() {
        return this.tenure;
    }
    
    public void setTenure(Tenure tenure) {
        this.tenure = tenure;
    }
}
