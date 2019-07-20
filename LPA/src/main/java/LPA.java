import java.util.Scanner;
import java.util.Map;
import java.util.*;

public class LPA{
    private static int times = 10;
    public static Map<String, Integer> name_label = new HashMap<String, Integer>();

    public static void main(String[] Args)throws Exception{

        System.out.println("File Read Path:");
        Scanner sc1 = new Scanner(System.in);
        String path_in = sc1.nextLine();
        System.out.println("File Written Path:");
        Scanner sc2 = new Scanner(System.in);
        String path_out = sc2.nextLine();

        String N = String.valueOf(1255);   //number of initial labels
        String[] forGB = {path_in, path_out + "Data0"};
        FileEdit.main(forGB);

        String[] forItr = {"", "", N};
        for(int i = 0; i < times; i++){
            forItr[0] = path_out + "Data" + i;
            forItr[1] = path_out + "Data" + String.valueOf(i + 1);
            LabelSet.main(forItr);
        }
        String[] forRV = {path_out + "Data" + times, path_out + "FinalLabel"};
        FileOutput.main(forRV);
    }
}