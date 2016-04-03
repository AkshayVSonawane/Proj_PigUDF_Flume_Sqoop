package udf_ComputePercentage;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ComputePercentage extends EvalFunc<Tuple> {

	public Tuple exec(Tuple input) throws IOException {

        if (input == null || input.size() == 0)

            return null;

       try{

    	   	Tuple output = TupleFactory.getInstance().newTuple(2);
    	   	output.set(0, input.get(1));

            int Project_Objectives_IHHL_BPL = (int) input.get(2);
            int Project_Objectives_IHHL_APL = (int) input.get(3);
            int Project_Objectives_IHHL_TOTAL = (int) input.get(4);
            int Project_Objectives_SCW = (int) input.get(5);
            int Project_Objectives_School_Toilets = (int) input.get(6);
            int Project_Objectives_Anganwadi_Toilets = (int) input.get(7);
            int Project_Objectives_RSM = (int) input.get(8);
            int Project_Objectives_PC = (int) input.get(9);
            int Project_Performance_IHHL_BPL = (int) input.get(10);
            int Project_Performance_IHHL_APL = (int) input.get(11);
            int Project_Performance_IHHL_TOTAL = (int) input.get(12);
            int Project_Performance_SCW = (int) input.get(13);
            int Project_Performance_School_Toilets = (int) input.get(14);
            int Project_Performance_Anganwadi_Toilets = (int) input.get(15);
            int Project_Performance_RSM = (int) input.get(16);
            int Project_Performance_PC = (int) input.get(17);
            
            float Project_Objectives=Project_Objectives_IHHL_TOTAL+Project_Objectives_SCW+Project_Objectives_School_Toilets+Project_Objectives_Anganwadi_Toilets+Project_Objectives_RSM+Project_Objectives_PC;
            float Project_Performance=Project_Performance_IHHL_TOTAL+Project_Performance_SCW+Project_Performance_School_Toilets+Project_Performance_Anganwadi_Toilets+Project_Performance_RSM+Project_Performance_PC;
            
            double Project_Percentage = ((Project_Performance/Project_Objectives)*100);
            output.set(1,Project_Percentage);
            
            
            
            return output;
            //store district_80_Percentage into '/Akshay/proj/pig_result7/' using PigStorage(',');

        }catch(Exception e){

            throw new IOException("Caught exception processing input row ", e);
        }
	}

}
