import java.util.Scanner;

public class MenPagerank{
    private static int times = 10;  //count for Iteration

    public static void main(String[] args)throws Exception{
        System.out.println("File Read Path:");
        Scanner sc1 = new Scanner(System.in);
        String path_in = sc1.nextLine();
        System.out.println("File Written Path:");
        Scanner sc2 = new Scanner(System.in);
        String path_out = sc2.nextLine();
        System.out.println("Number of urls:");
        Scanner sc3 = new Scanner(System.in);
        String url_number = sc3.nextLine();

        String[] forGB = {path_in, path_out + "Data0", url_number};
        GraphBuilder.main(forGB);

        String[] forItr = {"", ""};
        for(int i = 0; i < times; i++){
            forItr[0] = path_out + "Data" + String.valueOf(i);
            forItr[1] = path_out + "Data" + String.valueOf(i + 1);
            PageRankIter.main(forItr);
        }
        String[] forRV = {path_out + "Data" + String.valueOf(times), path_out + "FinalRank"};
        PageRankViewer.main(forRV);
    }
}