package cs455.hadoop.census.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import cs455.hadoop.census.record.*;
import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives BookRecord, list<word> pairs.
 * Emits sorted <long, ""> sorted pairs.
 */
public class CensusReducerQ2 extends Reducer<Text, BookRecord, Text, IntWritable> {

	//Book record that gets written to the context from combined values from all records from same state
    private static BookRecord theRecord = null; //the actual book of records we need to write to the context
	
    private static BookData theData = null; //the data initializer for BookRecord constructor
    private static Population thePopulation = null; //initializer for BookData constructor
    private static MaritalStatus theMaritalStatus = null; //initializer for BookData constructor
    private static Tenure theTenure = null; //initializer for BookRecord constructor

	//have to combine again to get full data for one state
    public void combine(Text key, Iterable<BookRecord> values){
			//these are the variables with the combined values of all records of same key, initialy set to zero
	IntWritable population = new IntWritable();	
	IntWritable [] popurbanvsrural = new IntWritable[2];
	for(int i = 0; i < popurbanvsrural.length; i++){
		popurbanvsrural[i] = new IntWritable(0);
	}
	IntWritable [] sex = new IntWritable[2];
	for(int i = 0; i < sex.length; i++){
		sex[i] = new IntWritable(0);
	}
	IntWritable [] age = new IntWritable[31];
	for(int i = 0; i < age.length; i++){
		age[i] = new IntWritable(0);
	}
	IntWritable [][] agebygender = new IntWritable[2][31];
	for(int i = 0; i < agebygender.length; i++){
		for(int j = 0; j < agebygender[i].length; j++){
			agebygender[i][j] = new IntWritable(0);
		}
	}
	IntWritable [] malemaritalstatus = new IntWritable[4];
	for(int i = 0; i < malemaritalstatus.length; i++){
		malemaritalstatus[i] = new IntWritable(0);
	}
	IntWritable [] femalemaritalstatus = new IntWritable[4];
	for(int i = 0; i < femalemaritalstatus.length; i++){
		femalemaritalstatus[i] = new IntWritable(0);
	}
	IntWritable [] occupied = new IntWritable[2];
	for(int i = 0; i < occupied.length; i++){
		occupied[i] = new IntWritable(0);
	}
	IntWritable [] houseurbanvsrural = new IntWritable[4];
	for(int i = 0; i < houseurbanvsrural.length; i++){
		houseurbanvsrural[i] = new IntWritable(0);
	}
	IntWritable [] rooms = new IntWritable[9];
	for(int i = 0; i < rooms.length; i++){
		rooms[i] = new IntWritable(0);
	}
	IntWritable [] value = new IntWritable[20];
	for(int i = 0; i < value.length; i++){
		value[i] = new IntWritable(0);
	}
	IntWritable [] contract = new IntWritable[17];
	for(int i = 0; i < contract.length; i++){
		contract[i] = new IntWritable(0);
	}
	//done initializing adding variables

	//now start to go through each val and add together
        for(BookRecord val : values){

				//go through and ADD IntWritables for every field in the population class
				population = new IntWritable(population.get() + val.getData().getPopulation().getPersons().get());

				//set for each field in the IntWritable []
				for(int i = 0; i < popurbanvsrural.length; i++){
					popurbanvsrural[i] = new IntWritable(popurbanvsrural[i].get() + val.getData().getPopulation().getUrbanRural()[i].get());
				}

				for(int i = 0; i < sex.length; i++){
					sex[i] = new IntWritable(sex[i].get() + val.getData().getPopulation().getSex()[i].get());
				}

				for(int i = 0; i < age.length; i++){
					age[i] = new IntWritable(age[i].get() + val.getData().getPopulation().getAge()[i].get());
				}

				for(int i = 0; i < agebygender.length; i++){
					for(int j = 0; j < agebygender[i].length; j++){
						agebygender[i][j] = new IntWritable(agebygender[i][j].get() + val.getData().getPopulation().getAgeByGender()[i][j].get());
					}
				}
				//now go through and add all values for marital status
				for(int i = 0; i < malemaritalstatus.length; i++){
					malemaritalstatus[i] = new IntWritable(malemaritalstatus[i].get() + val.getData().getMaritalStatus().getMale()[i].get());
				}
				for(int i = 0; i < femalemaritalstatus.length; i++){
					femalemaritalstatus[i] = new IntWritable(femalemaritalstatus[i].get() + val.getData().getMaritalStatus().getFemale()[i].get());
				}
				//now go through all values of Tenure and add together
				for(int i = 0; i < occupied.length; i++){
					occupied[i] = new IntWritable(occupied[i].get() + val.getData().getTenure().getOccupied()[i].get());
				}

				for(int i = 0; i < houseurbanvsrural.length; i++){
					houseurbanvsrural[i] = new IntWritable(houseurbanvsrural[i].get() + val.getData().getTenure().getUrbanRural()[i].get());
				}

				for(int i = 0; i < rooms.length; i++){
					rooms[i] = new IntWritable(rooms[i].get() + val.getData().getTenure().getRooms()[i].get());
				}

				for(int i = 0; i < value.length; i++){
					value[i] = new IntWritable(value[i].get() + val.getData().getTenure().getValue()[i].get());
				}

				for(int i = 0; i < contract.length; i++){
					contract[i] = new IntWritable(contract[i].get() + val.getData().getTenure().getContractRent()[i].get());
				}
				
        }
	thePopulation = new Population(population, popurbanvsrural, sex, age, agebygender); //we have all fields needed for population so we initialize
	theMaritalStatus = new MaritalStatus(malemaritalstatus, femalemaritalstatus); //we have all fields needed for MaritalStatus so we initialize
	theTenure = new Tenure(occupied, houseurbanvsrural, rooms, value, contract); //we have all fields needed for Tenure so we initialize
        theData = new BookData(thePopulation, theMaritalStatus, theTenure); //create the data for the record
	theRecord = new BookRecord(key, new IntWritable(0), theData); //now create the record that we can write
    }
   
//should only have one value in values due to combiner merging all records for each state, should be 50 keys
    @Override
    protected void reduce(Text key, Iterable<BookRecord> values, Context context) throws IOException, InterruptedException {
		combine(key,values);
		double numberOfMales = 0;
		double numberOfFemales = 0;

		IntWritable [] marriageMales = theRecord.getData().getMaritalStatus().getMale(); //get the array of male marriage statistics
		IntWritable [] marriageFemales = theRecord.getData().getMaritalStatus().getFemale(); //get the array of male marriage statistics

		double males = marriageMales[0].get(); //number of males never married
		double females = marriageFemales[0].get(); //number of females never married

		numberOfMales = marriageMales[0].get() + marriageMales[1].get() + marriageMales[2].get() + marriageMales[3].get(); //total number
		numberOfFemales = marriageFemales[0].get() + marriageFemales[1].get() + marriageFemales[2].get() + marriageFemales[3].get(); //total number
		int malePercent = (int) Math.round((males/numberOfMales) * 100); //now get the percentage in int form
		int femalePercent = (int) Math.round((females/numberOfFemales) * 100);

		//get some text to write
		Text male = new Text(key.toString() + " percentage male never married: %");
		Text female = new Text(key.toString() + " percentage female never married: %");
		//have to write both percentages
		context.write(male, new IntWritable(malePercent));
		context.write(female, new IntWritable(femalePercent));
		theRecord = null; //clear
		theData = null; //clear
		thePopulation = null; //clear
		theMaritalStatus = null; //clear
		theTenure = null; //clear

    }
}
