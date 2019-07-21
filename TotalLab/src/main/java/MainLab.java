import java.util.Scanner;

public class MainLab{
    public static Integer N = 0;

    public static void main(String[] args) throws Exception{
        //System.out.println("Enter Task Number(Following):");
        System.out.println("File Read Path:");
        Scanner sc1 = new Scanner(System.in);
        String input = sc1.nextLine();
        System.out.println("File Written Path:");
        Scanner sc2 = new Scanner(System.in);
        String output = sc2.nextLine();

        int val1 = 0, val2 = 0, val3 = 0, val4 = 0, val5 = 0, val6 = 0;
        int val = 0;
        while(val6 == 0){
            System.out.println("Enter Task Number(Following):");
            Scanner sc = new Scanner(System.in);
            val = sc.nextInt();
            if(val == 1){
                if(val1 == 1){
                    System.out.println("Please Enter Another Number(The Task has been completed!);");
                }else {
                    val1 = 1;
                    String[] argslist = {input, output + "output1/"};
                    WordConcurrence.main(argslist);
                    //System.out.println("Num1\r");
                }
            }else if(val == 2){
                if(val1 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    if(val2 == 1){
                        System.out.println("Please Enter Another Number(The Task has been completed!);");
                    }else {
                        val2 = 1;
                        String[] argslist = {output + "output1/*.txt.segmented", output + "output2/"};
                        MenPairs.main(argslist);
                        //System.out.println("Num2\r");
                    }
                }
            }else if(val == 3){
                if(val1 == 0 || val2 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    if(val3 == 1){
                        System.out.println("Please Enter Another Number(The Task has been completed!);");
                    }else {
                        val3 = 1;
                        String[] argslist = {output + "output2/part-r-00000", output + "output3/"};
                        MenMap.main(argslist);
                        //System.out.println("Num3\r");
                    }
                }
            }else if(val == 4){
                if(val1 == 0 || val2 == 0 || val3 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    if(val4 == 1){
                        System.out.println("Please Enter Another Number(The Task has been completed!);");
                    }else {
                        val4 = 1;
                        //System.out.println(N.toString());
                        String[] argslist = {output + "output3/part-r-00000", output + "output4/", N.toString()};
                        MenPagerank.main(argslist);
                        //System.out.println("Num4\r");
                    }
                }
            }else if(val == 5){
                if(val1 == 0 || val2 == 0 || val3 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    if(val5 == 1){
                        System.out.println("Please Enter Another Number(The Task has been completed!);");
                    }else {
                        val5 = 1;
                        String[] argslist = {output + "output3/part-r-00000", output + "output5/"};
                        LPA.main(argslist);
                        //System.out.println("Num5\r");
                    }
                }
            }else if(val == 6){
                if(val1 == 0 || val2 == 0 || val3 == 0 || val5 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    if(val6 == 1){
                        System.out.println("Please Enter Another Number(The Task has been completed!);");
                    }else {
                        val6 = 1;
                        String[] argslist = {output + "output5/FinalLabel/part-r-00000", output + "output6/"};
                        LabelProcess.main(argslist);
                        //System.out.println("Num6\r");
                    }
                }
            }else{
                System.out.println("Please Enter Another Number(The Input is wrong!);");
            }
        }
    }
}