import java.util.Scanner;
import java.util.Map;
import java.util.*;

public class LPA{
    private static int times = 10;
    public static Map<String, Integer> name_label = new HashMap<String, Integer>();

    public static void main(String[] Args)throws Exception{

        /*System.out.println("File Read Path:");
        Scanner sc1 = new Scanner(System.in);
        String path_in = sc1.nextLine();
        System.out.println("File Written Path:");
        Scanner sc2 = new Scanner(System.in);
        String path_out = sc2.nextLine();*/

        String N = MainLab.N.toString();   //number of initial labels
        String[] forGB = {Args[0], Args[1] + "Data0"};
        FileEdit.main(forGB);

        String[] forItr = {"", "", N};
        for(int i = 0; i < times; i++){
            forItr[0] = Args[1] + "Data" + i;
            forItr[1] = Args[1] + "Data" + String.valueOf(i + 1);
            LabelSet.main(forItr);
        }
        String[] forRV = {Args[1] + "Data" + times, Args[1] + "FinalLabel"};
        FileOutput.main(forRV);
    }
}