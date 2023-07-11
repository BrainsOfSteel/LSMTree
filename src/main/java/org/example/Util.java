package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.zip.CRC32;

public class Util {
    public static String SST_TABLE_PREFIX = "SST_";
    public static Integer ELEMENT_LIMIT = 1500;
    public static String SST_TABLE_DIRECTORY = "SstDirectory";

    public static void crcChecks(String crcString, String content, CRC32 crc32){
        long crcFromFile = Long.parseLong(crcString);
        crc32.update((content+"\n").getBytes());
        long crcFromContent = crc32.getValue();
        assert crcFromFile == crcFromContent;
    }

    public static void runSanity(String fileName){
        int count = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(fileName))){
            String line = null;
            int lastInd = -1;
            while((line = br.readLine()) != null){
                String lineContent[] = line.split("\\|");
                int key = Integer.parseInt(lineContent[1].split(",")[1]);
                if(lastInd +1 == key){
                    lastInd = key;
                }
                else{
                    System.out.println("Missed = " + (lastInd+1));
                    lastInd = key;
                }
                count++;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("count = "+count);
    }
}
