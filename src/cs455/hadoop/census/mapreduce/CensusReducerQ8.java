package cs455.hadoop.census.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import cs455.hadoop.census.record.*;
import java.io.IOException;
import java.util.Arrays;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives BookRecord, list<word> pairs.
 * Emits sorted <long, ""> sorted pairs.
 */
public class CensusReducerQ8 extends Reducer<Text, BookRecord, Text, Text> {

    //Book record that gets written to the context from combined values from all records from same state
    private static BookRecord theRecord = null; //the actual book of records we need to write to the context
	
    private static BookData theData = null; //the data initializer for BookRecord constructor
    private static Population thePopulation = null; //initializer for BookData constructor
    private static MaritalStatus theMaritalStatus = null; //initializer for BookData constructor
    private static Tenure theTenure = null; //initializer for BookRecord constructor

    private static int stateCounter = 0;
    private int [] elderlyPercentage = new int[51];
    private Text [] stateList = new Text[51];

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
    public void calculateElderly(BookRecord record){


				IntWritable [] age = record.getData().getPopulation().getAge();
				//get data from record of state and add to state pool rooms array
				double totalPeople = 0;
				for(int i = 0; i < age.length; i++){
					totalPeople += age[i].get();
				}
				int elderPercent = (int) Math.round((age[30].get()/totalPeople) * 100); //now get the percentage in int form
				elderlyPercentage[stateCounter] = elderPercent;
				stateCounter++;

    }

    @Override
    protected void reduce(Text key, Iterable<BookRecord> values, Context context) throws IOException, InterruptedException {

		combine(key,values);
		stateList[stateCounter] = key;
		calculateElderly(theRecord);
		
		if(stateCounter == 51){
			Text first = new Text("State with the highest percentage of elderly people: ");

			//now find the the index in roomsTotal with the same number as theNumber
			int biggestNumber = 0;
			int index = 0;
			for(int i = 0; i < elderlyPercentage.length; i++){
				if(elderlyPercentage[i] > biggestNumber){
					index = i;
					biggestNumber = elderlyPercentage[i];
				}
			}

			//have to write both percentages
			context.write(first, stateList[index]);
		}
		theRecord = null; //clear
		theData = null; //clear
		thePopulation = null; //clear
		theMaritalStatus = null; //clear
		theTenure = null; //clear
    }
}
