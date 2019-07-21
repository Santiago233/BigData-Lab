import java.util.Scanner;

public class MainLab{
    public static void main(String[] args) throws Exception{
        //System.out.println("Enter Task Number(Following):");
        int val1 = 0, val2 = 0, val3 = 0, val4 = 0, val5 = 0, val6 = 0;
        int val = 0;
        while(val6 == 0){
            System.out.println("Enter Task Number(Following):");
            Scanner sc = new Scanner(System.in);
            val = sc.nextInt();
            if(val == 1){
                val1 = 1;
                WordConcurrence.main(null);
                //System.out.println("Num1\r");
            }else if(val == 2){
                if(val1 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    val2 = 1;
                    MenPairs.main(null);
                    //System.out.println("Num2\r");
                }
            }else if(val == 3){
                if(val1 == 0 || val2 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    val3 = 1;
                    MenMap.main(null);
                    //System.out.println("Num3\r");
                }
            }else if(val == 4){
                if(val1 == 0 || val2 == 0 || val3 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    val4 = 1;
                    MenPagerank.main(null);
                    //System.out.println("Num4\r");
                }
            }else if(val == 5){
                if(val1 == 0 || val2 == 0 || val3 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    val5 = 1;
                    LPA.main(null);
                    //System.out.println("Num5\r");
                }
            }else if(val == 6){
                if(val1 == 0 || val2 == 0 || val3 == 0 || val5 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!);");
                }else{
                    val6 = 1;
                    LabelProcess.main(null);
                    //System.out.println("Num6\r");
                }
            }else{
                System.out.println("Please Enter Another Number(The Input is wrong!);");
            }
        }
    }
}