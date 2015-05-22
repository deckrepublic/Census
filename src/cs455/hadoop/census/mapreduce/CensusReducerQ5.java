package cs455.hadoop.census.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import cs455.hadoop.census.record.*;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives BookRecord, list<word> pairs.
 * Emits sorted <long, ""> sorted pairs.
 */
public class CensusReducerQ5 extends Reducer<Text, BookRecord, Text, Text> {
  
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
  

    @Override
    protected void reduce(Text key, Iterable<BookRecord> values, Context context) throws IOException, InterruptedException {
		combine(key, values);
		int numberOfHouses = 0;
		IntWritable [] houses = theRecord.getData().getTenure().getValue(); //get the array of house value statistics
		//go through and get total number of houses
		for(int i = 0; i < houses.length; i++){
			numberOfHouses += houses[i].get();
		}
		int houseCounter = 0; //a counter that will add up, when the middle house value is less than the next house counter that means the median range falls in that value group 
		int middleHouse = numberOfHouses/2; //middle house number
		int middleIndex = -1; //the index that the middle house falls in to determine value range

		//now go through and get middle index
		for(int i = 0; i < houses.length; i++){
			houseCounter += houses[i].get();
			if (middleHouse < houseCounter){
				middleIndex = i;
				break;
			}
		}
		Text [] ranges = { new Text("Less than $15,000"),new Text("$15,000 - $19,999"),new Text("$20,000 - $24,999"),new Text("$25,000 - $29,999"),new Text("$30,000 - $34,999"),
				new Text("$35,000 - $39,999"),new Text("$40,000 - $44,999"),new Text("$45,000 - $49,999"),new Text("$50,000 - $59,999"),new Text("$60,000 - $74,999"),
				new Text("$75,000 - $99,999 "),new Text("$100,000 - $124,999"),new Text("$125,000 - $149,999"),new Text("$150,000 - $174,999"),new Text("$175,000 - $199,999"),
				new Text("$200,000 - $249,999"),new Text("$250,000 - $299,999"),new Text("$300,000 - $399,999"),new Text("$400,000 - $499,999"),new Text("$500,000 or more")};

		//get some text to write
		Text first = new Text(key.toString() + " median value range for owned homes: ");
		//have to write both percentages
		context.write(first, ranges[middleIndex]);
		theRecord = null; //clear
		theData = null; //clear
		thePopulation = null; //clear
		theMaritalStatus = null; //clear
		theTenure = null; //clear

    }
}
