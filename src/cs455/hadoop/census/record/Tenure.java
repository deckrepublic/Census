package cs455.hadoop.census.record;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//first of three fields in book record, population tracks population numbers
public class Tenure implements Writable{
	private IntWritable [] occupied;
	private IntWritable [] urban_rural;
	private IntWritable [] rooms;
	private IntWritable [] value;
	private IntWritable [] contract_rent;
	
	//Default Constructor, array size determined by census table 
	public Tenure(){
		this.occupied = new IntWritable[2];
		this.urban_rural = new IntWritable[4];
		this.rooms = new IntWritable[9];
		this.value = new IntWritable[20];
		this.contract_rent = new IntWritable[17];
		for(int i = 0; i < this.occupied.length; i++){
			this.occupied[i] = new IntWritable();
		}
		for(int i = 0; i < this.urban_rural.length; i++){
			this.urban_rural[i] = new IntWritable();
		}
		for(int i = 0; i < this.rooms.length; i++){
			this.rooms[i] = new IntWritable();
		}
		for(int i = 0; i < this.value.length; i++){
			this.value[i] = new IntWritable();
		}
		for(int i = 0; i < this.contract_rent.length; i++){
			this.contract_rent[i] = new IntWritable();
		}
	}
	//my constructor
	public Tenure(IntWritable [] occupied, IntWritable [] urban_rural, 
			IntWritable [] rooms, IntWritable [] value, IntWritable [] contract_rent){
		this.occupied = occupied;
		this.urban_rural = urban_rural;
		this.rooms = rooms;
		this.value = value;
		this.contract_rent = contract_rent;
	}
	//Serialize the data fields 
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		for(int i = 0; i < this.occupied.length; i++){
			this.occupied[i].write(dataOutput);
		}
		for(int i = 0; i < this.urban_rural.length; i++){
			this.urban_rural[i].write(dataOutput);
		}
		for(int i = 0; i < this.rooms.length; i++){
			this.rooms[i].write(dataOutput);
		}
		for(int i = 0; i < this.value.length; i++){
			this.value[i].write(dataOutput);
		}
		for(int i = 0; i < this.contract_rent.length; i++){
			this.contract_rent[i].write(dataOutput);
		}
	}
	//de-Serialize the byte stream into data fields
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		for(int i = 0; i < this.occupied.length; i++){
			this.occupied[i].readFields(dataInput);
		}
		for(int i = 0; i < this.urban_rural.length; i++){
			this.urban_rural[i].readFields(dataInput);
		}
		for(int i = 0; i < this.rooms.length; i++){
			this.rooms[i].readFields(dataInput);
		}
		for(int i = 0; i < this.value.length; i++){
			this.value[i].readFields(dataInput);
		}
		for(int i = 0; i < this.contract_rent.length; i++){
			this.contract_rent[i].readFields(dataInput);
		}
	}
	//getters and setters
    public IntWritable [] getOccupied() {
        return this.occupied;
    }
    
    public void setOccupied(IntWritable [] occupied) {
        this.occupied = occupied;
    }
    
    public IntWritable [] getUrbanRural() {
        return this.urban_rural;
    }
    
    public void setUrbanRural(IntWritable [] urban_rural) {
        this.urban_rural = urban_rural;
    }
    
    public IntWritable [] getRooms() {
        return this.rooms;
    }
    
    public void setRooms(IntWritable [] rooms) {
        this.rooms = rooms;
    }
    
    public IntWritable [] getValue() {
        return this.value;
    }
    
    public void setValue(IntWritable [] value) {
        this.value = value;
    }
    
    public IntWritable [] getContractRent() {
        return this.contract_rent;
    }
    
    public void setContractRent(IntWritable [] contract_rent) {
        this.contract_rent = contract_rent;
    }
}
