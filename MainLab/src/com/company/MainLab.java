package com.company;
import java.io.IOException;
import java.util.Scanner;

public class MainLab{
    public static void main(String[] args) throws Exception{
        System.out.println("Enter Task Number(Following):");
        int val1 = 0, val2 = 0, val3 = 0, val4 = 0;
        int val = 0;
        while(val4 == 0){
            Scanner sc = new Scanner(System.in);
            val = sc.nextInt();
            if(val == 1){
                val1 = 1;
                //WordConcurrence.main();
                System.out.println("Numer1\r");
            }else if(val == 2){
                if(val1 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!):");
                }else{
                    val2 = 1;
                    //MenPairs.main();
                    System.out.println("Numer2\r");
                }
            }else if(val == 3){
                if(val1 == 0 || val2 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!):");
                }else{
                    val3 = 1;
                    //MenMap.main();
                    System.out.println("Numer3\r");
                }
            }else if(val == 4){
                if(val1 == 0 || val2 == 0 || val3 == 0){
                    System.out.println("Please Enter Another Number(The Task now is not Supported!):");
                }else{
                    val4 = 1;
                    //MenPagerank.main();
                    System.out.println("Numer4\r");
                }
            }else{
                //TODO
            }
        }
    }
}